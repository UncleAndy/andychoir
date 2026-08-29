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
    // Сначала переводим LaTeX-конструкции в Unicode, т.к. termimad их не понимает.
    let prepped = latex_to_unicode(markdown);
    let text = termimad::MadSkin::default_dark().term_text(&prepped).to_string();
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

/// Лёгкий перевод распространённых LaTeX-конструкций в Unicode-символы, чтобы
/// формулы были читаемы в терминале (termimad их не понимает). Это НЕ полный
/// LaTeX-рендер: обрабатываются частые команды (греческие буквы, стрелки,
/// \frac, \sqrt, индексы/степени и т.п.).
fn latex_to_unicode(src: &str) -> String {
    let mut s = src.to_string();

    // 1. Греческие буквы и частые символы (простые замены).
    let greek: &[(&str, &str)] = &[
        ("\\alpha", "α"), ("\\beta", "β"), ("\\gamma", "γ"), ("\\delta", "δ"),
        ("\\epsilon", "ε"), ("\\zeta", "ζ"), ("\\eta", "η"), ("\\theta", "θ"),
        ("\\lambda", "λ"), ("\\mu", "μ"), ("\\nu", "ν"), ("\\xi", "ξ"),
        ("\\pi", "π"), ("\\rho", "ρ"), ("\\sigma", "σ"), ("\\tau", "τ"),
        ("\\phi", "φ"), ("\\chi", "χ"), ("\\psi", "ψ"), ("\\omega", "ω"),
        ("\\Gamma", "Γ"), ("\\Delta", "Δ"), ("\\Theta", "Θ"), ("\\Lambda", "Λ"),
        ("\\Pi", "Π"), ("\\Sigma", "Σ"), ("\\Phi", "Φ"), ("\\Psi", "Ψ"),
        ("\\Omega", "Ω"),
        ("\\rightarrow", "→"), ("\\leftarrow", "←"), ("\\Rightarrow", "⇒"),
        ("\\Leftarrow", "⇐"), ("\\leftrightarrow", "↔"),
        ("\\infty", "∞"), ("\\partial", "∂"), ("\\nabla", "∇"),
        ("\\hbar", "ℏ"), ("\\cdot", "·"), ("\\times", "×"), ("\\pm", "±"),
        ("\\div", "÷"), ("\\sum", "∑"), ("\\prod", "∏"), ("\\int", "∫"),
        ("\\leq", "≤"), ("\\geq", "≥"), ("\\approx", "≈"), ("\\neq", "≠"),
        ("\\in", "∈"), ("\\subset", "⊂"), ("\\subseteq", "⊆"),
        ("\\cup", "∪"), ("\\cap", "∩"), ("\\forall", "∀"), ("\\exists", "∃"),
        ("\\sqrt", "√"), ("\\dots", "…"), ("\\ldots", "…"), ("\\ " , " "),
        ("\\quad", "  "), ("\\qquad", "    "),
        // текст: \text{Cloud_V2} -> Cloud_V2 (убираем команду и обрамляющие {}).
        // Обрабатывается в отдельной функции (нужна парная }).
        ("\\operatorname{", ""),
        // скобки \left[ \right] -> [ ]
        ("\\left[", "["), ("\\right]", "]"), ("\\left(", "("), ("\\right)", ")"),
        ("\\left", ""), ("\\right", ""),
        ("\\_", "_"),
    ];
    for (cmd, repl) in greek {
        s = s.replace(cmd, repl);
    }

    // 1.5. \text{...} -> содержимое (убираем команду и парную {}).
    s = replace_text_group(&s);

    // 2. \frac{num}{den} -> num/den
    s = replace_frac(&s);

    // 3. ^{...} и _{...} -> индекс/степень (содержимое оставляем как есть).
    s = replace_sub_sup(&s, "^");
    s = replace_sub_sup(&s, "_");

    // 4. Убираем маркеры формул $...$ и $$...$$ (оставляем содержимое).
    s = s.replace("$$", "");
    s = s.replace("$", "");

    s
}

/// Заменить команды стиля/шрифта вида \mathbf{...}, \text{...}, \mathrm{...} и
/// т.п. на их содержимое (убрать команду и парную {}). Обрабатываются только
/// команды из списка (frac и пр. с особым поведением — отдельно).
fn replace_text_group(s: &str) -> String {
    // Команды, чей аргумент {..} заменяется на содержимое.
    const CMDS: &[&str] = &[
        "mathbf", "text", "mathrm", "mathit", "boldsymbol", "bm", "vec", "hat",
        "bar", "overline", "underline", "mathrm", "mathtt", "mathsf", "mathbb",
        "mathcal", "mathscr", "mathfrak",
    ];
    let mut out = String::new();
    let mut rest = s;
    loop {
        let Some(bs) = rest.find('\\') else { break };
        out.push_str(&rest[..bs]);
        rest = &rest[bs..];
        if rest.len() < 2 { out.push('\\'); break; }
        rest = &rest[1..];
        let name_end = rest
            .char_indices()
            .find(|(_, c)| !c.is_ascii_alphabetic())
            .map(|(i, _)| i)
            .unwrap_or(rest.len());
        let name = &rest[..name_end];
        if name.is_empty() {
            out.push('\\');
            continue;
        }
        rest = &rest[name_end..];
        // Обрабатываем только если команда в списке И сразу '{'.
        if CMDS.contains(&name) && rest.starts_with('{') {
            rest = &rest[1..];
            if let Some(close) = rest.find('}') {
                out.push_str(&rest[..close]);
                rest = &rest[close + 1..];
                continue;
            } else {
                out.push('\\');
                out.push_str(name);
                out.push('{');
                out.push_str(rest);
                break;
            }
        } else {
            out.push('\\');
            out.push_str(name);
            continue;
        }
    }
    out.push_str(rest);
    out
}

/// Заменить \frac{num}{den} на "num/den" (однократно, вложенность не глубокая).
fn replace_frac(s: &str) -> String {
    let mut out = String::new();
    let mut rest = s;
    while let Some(pos) = rest.find("\\frac{") {
        out.push_str(&rest[..pos]);
        rest = &rest[pos + "\\frac{".len()..];
        // Читаем числитель до закрывающей } (без вложенных {}).
        if let Some(close) = rest.find('}') {
            let num = &rest[..close];
            rest = &rest[close + 1..];
            // Ожидаем {den}
            if let Some(dpos) = rest.find('{') {
                if dpos == 0 {
                    rest = &rest[1..];
                    if let Some(dclose) = rest.find('}') {
                        let den = &rest[..dclose];
                        rest = &rest[dclose + 1..];
                        out.push_str(num);
                        out.push('/');
                        out.push_str(den);
                        continue;
                    }
                }
            }
            // Не нашли знаменатель — возвращаем как было.
            out.push_str(num);
            out.push('}');
        } else {
            out.push_str("\\frac{");
            break;
        }
    }
    out.push_str(rest);
    out
}

/// Заменить ^{...} или _{...} на содержимое (без маркера, для читаемости).
fn replace_sub_sup(s: &str, marker: &str) -> String {
    let mut out = String::new();
    let mut rest = s;
    while let Some(pos) = rest.find(&format!("\\{}", marker)) {
        out.push_str(&rest[..pos]);
        rest = &rest[pos + 2..];
        if let Some(open) = rest.find('{') {
            if open == 0 {
                rest = &rest[1..];
                if let Some(close) = rest.find('}') {
                    out.push_str(&rest[..close]);
                    rest = &rest[close + 1..];
                    continue;
                }
            }
        }
        out.push_str(marker);
    }
    out.push_str(rest);
    out
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latex_simple_symbols() {
        assert_eq!(latex_to_unicode(r"$\rightarrow$"), "→");
        assert_eq!(latex_to_unicode(r"$\hbar$"), "ℏ");
        assert_eq!(latex_to_unicode(r"$\partial$"), "∂");
        assert_eq!(latex_to_unicode(r"$\nabla$"), "∇");
        assert_eq!(latex_to_unicode(r"$\alpha + \beta$"), "α + β");
    }

    #[test]
    fn latex_frac() {
        assert_eq!(latex_to_unicode(r"$\frac{a}{b}$"), "a/b");
        assert_eq!(latex_to_unicode(r"$\frac{2}{3}x$"), "2/3x");
    }

    #[test]
    fn latex_text_underscore() {
        // \text{Cloud_V2} -> Cloud_V2 (убираем команду и маркеры)
        assert_eq!(latex_to_unicode(r"$\text{Cloud\_V2}$"), "Cloud_V2");
    }

    #[test]
    fn latex_display_formula() {
        // Сложная формула: убираем маркеры $$, скобки, заменяем греческие.
        let src = r"$$i\hbar\frac{\partial}{\partial t}\Psi(\mathbf{r},t) = \left[-\frac{\hbar^2}{2m}\nabla^2\right]\Psi(\mathbf{r},t)$$";
        let out = latex_to_unicode(src);
        assert!(out.contains("ℏ"), "ожидался ℏ, получили: {}", out);
        assert!(out.contains("∂"), "ожидался ∂, получили: {}", out);
        assert!(out.contains("Ψ"), "ожидался Ψ, получили: {}", out);
        assert!(!out.contains("$$"), "маркеры $$ должны быть убраны: {}", out);
        // \mathbf{r} -> r (убираем команду шрифта)
        assert!(!out.contains("\\mathbf"), "\\mathbf должен быть убран: {}", out);
        assert!(out.contains("Ψ(r,t)"), "ожидался Ψ(r,t), получили: {}", out);
    }
}
