use std::fmt;
use std::future;
use std::sync::Mutex;
use std::{println as std_println};

use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, ExternalPrinter};
use tokio::sync::Notify;

static CONSOLE: Mutex<ConsoleState> = Mutex::new(ConsoleState {
    active_printer: None,
});

static EDITOR: Mutex<Option<DefaultEditor>> = Mutex::new(None);

static INTERRUPT: Notify = Notify::const_new();

struct ConsoleState {
    active_printer: Option<Box<dyn ExternalPrinter + Send>>,
}

pub fn print_line(args: fmt::Arguments<'_>) {
    let mut console = CONSOLE.lock().unwrap();
    // print_line() должен печатать ИМЕННО строку: с завершающим переводом.
    // rustyline ExternalPrinter печатает без \n, поэтому добавляем его сами —
    // иначе следующая строка (например промпт) затирает конец вывода.
    let text = format!("{}\n", args);

    if let Some(printer) = &mut console.active_printer {
        if let Err(err) = printer.print(text) {
            eprintln!("[Хост] Ошибка обновления консоли: {:?}", err);
        }
    } else {
        // Здесь префикс std у println! является обязательным! Иначе возникнет бесконечная рекурсия.
        std_println!("{}", args);
    }
}

/// Напечатать Markdown с форматированием (через termimad → ANSI). Вызывается
/// хостом, когда front:console хочет показать ответ как Markdown.
/// Рендерим в ANSI-строку и выводим через тот же путь, что print_line
/// (rustyline ExternalPrinter, если консоль активна; иначе stdout) — чтобы
/// форматированный вывод не ломал отрисовку промпта.
pub fn print_markdown(markdown: &str) {
    // term_text возвращает FmtText (тип с Display) — конвертируем в String.
    let text = termimad::MadSkin::default_dark().term_text(markdown).to_string();
    crate::info!(
        "[Хост] Console: вывод Markdown через termimad ({} байт)",
        text.len()
    );
    let mut console = CONSOLE.lock().unwrap();
    if let Some(printer) = &mut console.active_printer {
        if let Err(err) = printer.print(text) {
            crate::error!("[Хост] Ошибка обновления консоли: {:?}", err);
        }
    } else {
        std_println!("{}", text);
    }
}

pub async fn read_prompted_line(prompt: String) -> Option<String> {
    let read_result = tokio::task::spawn_blocking(move || read_line_with_editor(prompt)).await;

    {
        let mut console = CONSOLE.lock().unwrap();
        console.active_printer = None;
    }

    match read_result {
        Ok(Ok(line)) => Some(line),
        Ok(Err(ReadlineError::Interrupted)) => {
            INTERRUPT.notify_waiters();
            future::pending().await
        }
        Ok(Err(ReadlineError::Eof)) => {
            INTERRUPT.notify_waiters();
            future::pending().await
        }
        Ok(Err(err)) => {
            eprintln!("[Хост] Ошибка чтения из stdin: {:?}", err);
            None
        }
        Err(err) => {
            eprintln!(
                "[Хост] Задача чтения из stdin завершилась с ошибкой: {:?}",
                err
            );
            None
        }
    }
}

pub async fn wait_for_interrupt() {
    INTERRUPT.notified().await;
}

fn read_line_with_editor(prompt: String) -> rustyline::Result<String> {
    let mut editor_lock = EDITOR.lock().unwrap();
    if editor_lock.is_none() {
        *editor_lock = Some(DefaultEditor::new()?);
    }

    let editor = editor_lock.as_mut().unwrap();
    {
        let mut console = CONSOLE.lock().unwrap();
        console.active_printer = Some(Box::new(editor.create_external_printer()?));
    }

    let line = editor.readline(&prompt)?;

    if !line.trim().is_empty() {
        let _ = editor.add_history_entry(line.as_str());
    }

    Ok(format!("{}\n", line))
}
