use std::fmt;
use std::io::{BufRead, Write};
use std::sync::Mutex;

static CONSOLE: Mutex<ConsoleState> = Mutex::new(ConsoleState {
    active_prompt: None,
});

struct ConsoleState {
    active_prompt: Option<String>,
}

pub fn print_line(args: fmt::Arguments<'_>) {
    let console = CONSOLE.lock().unwrap();

    if let Some(prompt) = &console.active_prompt {
        print!("\r\x1b[2K");
        // Здесь префикс std у println! является обязательным! Иначе возникнет бесконечная рекурсия.
        std::println!("{}", args);
        print!("{}", prompt);
        if let Err(err) = std::io::stdout().flush() {
            eprintln!("[Хост] Ошибка обновления консоли: {:?}", err);
        }
    } else {
        // Здесь префикс std у println! является обязательным! Иначе возникнет бесконечная рекурсия.
        std::println!("{}", args);
    }
}

pub async fn read_prompted_line(prompt: String) -> Option<String> {
    if !show_prompt(prompt) {
        return None;
    }

    let read_result = tokio::task::spawn_blocking(|| {
        let mut buffer = Vec::new();
        std::io::stdin()
            .lock()
            .read_until(b'\n', &mut buffer)
            .map(|read| (read, String::from_utf8_lossy(&buffer).into_owned()))
    })
    .await;

    let mut console = CONSOLE.lock().unwrap();
    console.active_prompt = None;
    drop(console);

    match read_result {
        Ok(Ok((0, _))) => None,
        Ok(Ok((_, buffer))) => Some(buffer),
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

fn show_prompt(prompt: String) -> bool {
    let mut console = CONSOLE.lock().unwrap();
    console.active_prompt = Some(prompt.clone());
    print!("{}", prompt);
    if let Err(err) = std::io::stdout().flush() {
        console.active_prompt = None;
        eprintln!("[Хост] Ошибка вывода prompt: {:?}", err);
        false
    } else {
        true
    }
}
