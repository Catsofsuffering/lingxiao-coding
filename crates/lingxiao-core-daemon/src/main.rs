use lingxiao_core_daemon::{build_info, serve_with_runtime_config, DAEMON_NAME};
use std::process;

fn print_version() {
    println!("{}", build_info());
}

fn print_usage(program: &str) {
    eprintln!("Usage: {program} [COMMAND]");
    eprintln!();
    eprintln!("Commands:");
    eprintln!("  version              Print version information");
    eprintln!("  help                 Print this help message");
    eprintln!("  serve --db <PATH> [--runtime-config <PATH>]");
    eprintln!("                       Start stdio JSON-lines daemon");
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let program = args.first().map(|s| s.as_str()).unwrap_or(DAEMON_NAME);

    let cmd = args.get(1).map(|s| s.as_str()).unwrap_or("help");

    match cmd {
        "version" | "--version" | "-V" => {
            print_version();
        }
        "help" | "--help" | "-h" => {
            print_usage(program);
            process::exit(0);
        }
        "serve" => {
            let db_path = parse_serve_db_path(&args).unwrap_or_else(|| {
                eprintln!("Error: serve requires --db <PATH>");
                print_usage(program);
                process::exit(1);
            });
            let runtime_config = parse_flag_value(&args, "--runtime-config");
            if let Err(e) = serve_with_runtime_config(&db_path, runtime_config.as_deref()) {
                eprintln!("Daemon error: {e}");
                process::exit(1);
            }
        }
        _ => {
            eprintln!("Unknown command: {cmd}");
            print_usage(program);
            process::exit(1);
        }
    }
}

fn parse_serve_db_path(args: &[String]) -> Option<String> {
    parse_flag_value(args, "--db")
}

fn parse_flag_value(args: &[String], flag: &str) -> Option<String> {
    args.windows(2).find(|w| w[0] == flag).map(|w| w[1].clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_daemon_name_constant() {
        assert_eq!(DAEMON_NAME, "lingxiao-core-daemon");
    }

    #[test]
    fn test_parse_serve_db_path_found() {
        let args = vec![
            "lingxiao-core-daemon".into(),
            "serve".into(),
            "--db".into(),
            "/tmp/test.db".into(),
        ];
        assert_eq!(parse_serve_db_path(&args), Some("/tmp/test.db".into()));
    }

    #[test]
    fn test_parse_serve_db_path_missing_flag() {
        let args = vec!["lingxiao-core-daemon".into(), "serve".into()];
        assert_eq!(parse_serve_db_path(&args), None);
    }

    #[test]
    fn test_parse_serve_db_path_missing_value() {
        let args = vec!["lingxiao-core-daemon".into(), "serve".into(), "--db".into()];
        assert_eq!(parse_serve_db_path(&args), None);
    }

    #[test]
    fn test_parse_runtime_config_path_found() {
        let args = vec![
            "lingxiao-core-daemon".into(),
            "serve".into(),
            "--db".into(),
            "/tmp/test.db".into(),
            "--runtime-config".into(),
            "/tmp/runtime.json".into(),
        ];
        assert_eq!(
            parse_flag_value(&args, "--runtime-config"),
            Some("/tmp/runtime.json".into())
        );
    }
}
