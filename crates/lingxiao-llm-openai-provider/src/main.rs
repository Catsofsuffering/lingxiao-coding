fn main() {
    if let Err(err) = lingxiao_llm_openai_provider::run_stdio() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}
