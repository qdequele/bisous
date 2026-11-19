//! `meili-connect`: keeps Meilisearch indexes in sync with databases.

#[tokio::main]
async fn main() -> std::process::ExitCode {
    match meili_connect_server::run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            // Tracing may not be initialised yet (config errors).
            eprintln!("meili-connect: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}
