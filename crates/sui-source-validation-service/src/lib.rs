// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use actix_web::{dev::Server, web, App, HttpRequest, HttpServer, Responder};
use anyhow::{anyhow, bail};
use serde::Deserialize;
use url::Url;

use move_package::BuildConfig as MoveBuildConfig;
use sui_move::build::resolve_lock_file_path;
use sui_move_build::{BuildConfig, SuiPackageHooks};
use sui_sdk::wallet_context::WalletContext;
use sui_source_validation::{BytecodeSourceVerifier, SourceMode};

#[derive(Deserialize, Debug)]
pub struct Config {
    pub packages: Vec<Packages>,
}

#[derive(Clone, Deserialize, Debug)]
pub struct Packages {
    repository: String,
    paths: Vec<String>,
}

pub async fn verify_package(
    context: &WalletContext,
    package_path: impl AsRef<Path>,
) -> anyhow::Result<()> {
    move_package::package_hooks::register_package_hooks(Box::new(SuiPackageHooks));
    let config = resolve_lock_file_path(
        MoveBuildConfig::default(),
        Some(package_path.as_ref().to_path_buf()),
    )
    .unwrap();
    let build_config = BuildConfig {
        config,
        run_bytecode_verifier: false, /* no need to run verifier if code is on-chain */
        print_diags_to_stderr: false,
    };
    let compiled_package = build_config
        .build(package_path.as_ref().to_path_buf())
        .unwrap();

    let client = context.get_client().await?;
    BytecodeSourceVerifier::new(client.read_api())
        .verify_package(&compiled_package, true, SourceMode::Verify)
        .await
        .map_err(anyhow::Error::from)
}

pub fn parse_config(config_path: impl AsRef<Path>) -> anyhow::Result<Config> {
    let contents = fs::read_to_string(config_path)?;
    toml::from_str(&contents).map_err(anyhow::Error::from)
}

pub async fn clone_packages(p: Packages, dir: PathBuf) -> anyhow::Result<()> {
    let repo_url = Url::parse(&p.repository)?;
    let Some(components) = repo_url.path_segments().map(|c| c.collect::<Vec<_>>()) else {
	bail!("Could not discover repository path in url {}", &p.repository)
    };
    let Some(repo_name) = components.last() else {
	bail!("Could not discover repository name in url {}", &p.repository)
    };
    let dest = dir
        .join(repo_name)
        .into_os_string()
        .into_string()
        .map_err(|_| {
            anyhow!(
                "Could not create path to clone repsository {}",
                &p.repository
            )
        })?;

    // Clone the empty repository.
    Command::new("git")
        .args([
            "clone",
            "-n",
            "--depth=1",
            "--filter=tree:0",
            &p.repository,
            &dest,
        ])
        .output()
        .map_err(|_| anyhow!("Could not clone repository {}", &p.repository))?;

    // Do a sparse check out for the package set.
    let mut args: Vec<String> = vec!["-C", &dest, "sparse-checkout", "set", "--no-cone"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    args.extend_from_slice(&p.paths);
    Command::new("git")
        .args(args)
        .output()
        .map_err(|_| anyhow!("could not sparse-checkout paths for {}", &p.repository))?;

    // Checkout the default branch.
    Command::new("git")
        .args(["-C", &dest, "checkout"])
        .output()
        .map_err(|_| anyhow!("could not checkout {}", &p.repository))?;
    Ok(())
}

pub async fn clone_repositories(config: &Config, dir: &Path) -> anyhow::Result<()> {
    let mut tasks = vec![];
    for p in &config.packages {
        let new_p = p.clone();
        let new_dir = PathBuf::from(dir);
        let t = tokio::spawn(async move { clone_packages(new_p, new_dir).await });
        tasks.push(t);
    }

    for t in tasks {
        t.await.unwrap()?;
    }
    Ok(())
}

pub async fn initialize(
    context: &WalletContext,
    config: &Config,
    dir: &Path,
) -> anyhow::Result<()> {
    clone_repositories(config, dir).await?;
    verify_packages(context, vec![]).await?;
    Ok(())
}

pub async fn verify_packages(
    context: &WalletContext,
    package_paths: Vec<PathBuf>,
) -> anyhow::Result<()> {
    for p in package_paths {
        verify_package(context, p).await?
    }
    Ok(())
}

pub fn serve() -> anyhow::Result<Server> {
    Ok(
        HttpServer::new(|| App::new().route("/api", web::get().to(api_route)))
            .bind("0.0.0.0:8000")?
            .run(),
    )
}

async fn api_route(_request: HttpRequest) -> impl Responder {
    "{\"source\": \"code\"}"
}
