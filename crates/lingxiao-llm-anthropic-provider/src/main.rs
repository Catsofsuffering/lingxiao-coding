fn main() {
    if let Err(err) = lingxiao_llm_anthropic_provider::run_stdio() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}
