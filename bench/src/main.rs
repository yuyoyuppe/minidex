use std::io::{BufRead, BufReader, Write, stdin, stdout};
use std::path::Path;
use std::time::Instant;

use minidex::{CompactorConfig, FilesystemEntry, Index, Kind, SearchOptions, VolumeType, category};

fn human_size(bytes: u64) -> String {
    const MIB: u64 = 1024 * 1024;
    const KIB: u64 = 1024;
    if bytes >= MIB {
        format!("{} MB", (bytes + MIB / 2) / MIB)
    } else if bytes >= KIB {
        format!("{} KB", (bytes + KIB / 2) / KIB)
    } else {
        format!("{bytes} B")
    }
}

fn process_memory() -> (u64, u64, u64) {
    #[cfg(windows)]
    {
        use std::mem::zeroed;
        #[repr(C)]
        #[allow(non_snake_case)]
        struct ProcessMemoryCountersEx {
            cb: u32,
            PageFaultCount: u32,
            PeakWorkingSetSize: usize,
            WorkingSetSize: usize,
            QuotaPeakPagedPoolUsage: usize,
            QuotaPagedPoolUsage: usize,
            QuotaPeakNonPagedPoolUsage: usize,
            QuotaNonPagedPoolUsage: usize,
            PagefileUsage: usize,
            PeakPagefileUsage: usize,
            PrivateUsage: usize,
        }
        unsafe extern "system" {
            fn GetCurrentProcess() -> *mut std::ffi::c_void;
            fn K32GetProcessMemoryInfo(
                process: *mut std::ffi::c_void,
                pmc: *mut ProcessMemoryCountersEx,
                cb: u32,
            ) -> i32;
        }
        unsafe {
            let mut pmc: ProcessMemoryCountersEx = zeroed();
            pmc.cb = size_of::<ProcessMemoryCountersEx>() as u32;
            K32GetProcessMemoryInfo(
                GetCurrentProcess(),
                &mut pmc,
                pmc.cb,
            );
            (
                pmc.WorkingSetSize as u64,
                pmc.PeakWorkingSetSize as u64,
                pmc.PrivateUsage as u64,
            )
        }
    }
    #[cfg(not(windows))]
    {
        (0, 0, 0)
    }
}

fn print_process_memory() {
    let (rss, peak, private) = process_memory();
    if rss > 0 {
        println!("\n--- Process memory ---");
        println!("Working set (RSS):      {}", human_size(rss));
        println!("Peak working set:       {}", human_size(peak));
        println!("Private bytes (commit): {}", human_size(private));
    }
}

/// Guess the volume from a path (e.g. "C:" from "C:\Windows\System32")
fn extract_volume(path: &str) -> &str {
    if path.len() >= 2 && path.as_bytes()[1] == b':' {
        &path[..2]
    } else if path.starts_with("\\\\") {
        // UNC path: \\server\share -> \\server\share
        if let Some(pos) = path[2..].find('\\') {
            let after_server = 2 + pos + 1;
            if let Some(pos2) = path[after_server..].find('\\') {
                &path[..after_server + pos2]
            } else {
                path
            }
        } else {
            path
        }
    } else {
        "/"
    }
}

/// Heuristic: if the last component has a dot, it's a file; otherwise directory
fn guess_kind(path: &str) -> Kind {
    let last = path.rsplit(['\\', '/']).next().unwrap_or(path);
    if last.contains('.') {
        Kind::File
    } else {
        Kind::Directory
    }
}

fn guess_category(path: &str) -> u8 {
    let lower = path.to_ascii_lowercase();
    if lower.ends_with(".jpg")
        || lower.ends_with(".jpeg")
        || lower.ends_with(".png")
        || lower.ends_with(".gif")
        || lower.ends_with(".bmp")
        || lower.ends_with(".svg")
        || lower.ends_with(".webp")
        || lower.ends_with(".ico")
    {
        category::IMAGE
    } else if lower.ends_with(".mp4")
        || lower.ends_with(".avi")
        || lower.ends_with(".mkv")
        || lower.ends_with(".mov")
        || lower.ends_with(".wmv")
    {
        category::VIDEO
    } else if lower.ends_with(".mp3")
        || lower.ends_with(".wav")
        || lower.ends_with(".flac")
        || lower.ends_with(".ogg")
        || lower.ends_with(".aac")
    {
        category::AUDIO
    } else if lower.ends_with(".doc")
        || lower.ends_with(".docx")
        || lower.ends_with(".pdf")
        || lower.ends_with(".xls")
        || lower.ends_with(".xlsx")
        || lower.ends_with(".ppt")
        || lower.ends_with(".pptx")
    {
        category::DOCUMENT
    } else if lower.ends_with(".zip")
        || lower.ends_with(".rar")
        || lower.ends_with(".7z")
        || lower.ends_with(".tar")
        || lower.ends_with(".gz")
    {
        category::ARCHIVE
    } else if lower.ends_with(".txt")
        || lower.ends_with(".md")
        || lower.ends_with(".log")
        || lower.ends_with(".csv")
        || lower.ends_with(".json")
        || lower.ends_with(".xml")
        || lower.ends_with(".yml")
        || lower.ends_with(".yaml")
        || lower.ends_with(".toml")
        || lower.ends_with(".ini")
        || lower.ends_with(".cfg")
    {
        category::TEXT
    } else {
        category::OTHER
    }
}

fn build_index(file_path: &str, index_dir: &str) -> Result<Index, Box<dyn std::error::Error>> {
    // Clean previous index
    if Path::new(index_dir).exists() {
        std::fs::remove_dir_all(index_dir)?;
    }

    let config = CompactorConfig {
        flush_threshold: 50_000,
        tombstone_threshold: 100,
        min_merge_count: 3,
    };
    let index = Index::open_with_config(index_dir, config)?;

    println!("Inserting paths...");
    let t0 = Instant::now();
    let mut count = 0u64;

    {
        let file = std::fs::File::open(file_path)?;
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line = line?;
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            let volume = extract_volume(line).to_string();
            let kind = guess_kind(line);
            let cat = guess_category(line);

            index.insert(FilesystemEntry {
                path: line.into(),
                volume,
                kind,
                last_modified: 0,
                last_accessed: 0,
                category: cat,
                volume_type: VolumeType::Local,
            })?;

            count += 1;
            if count % 100_000 == 0 {
                print!("  {} inserted...\r", count);
                stdout().flush()?;
            }
        }
    } // file + reader dropped here

    let insert_time = t0.elapsed();
    println!(
        "Inserted {} paths in {:.1}ms ({:.0} paths/sec)",
        count,
        insert_time.as_secs_f64() * 1000.0,
        count as f64 / insert_time.as_secs_f64()
    );

    // Flush and compact
    print!("Syncing and compacting... ");
    stdout().flush()?;
    let t0 = Instant::now();
    index.sync()?;
    index.force_compact_all()?;
    let compact_time = t0.elapsed();
    println!("done in {:.1}ms", compact_time.as_secs_f64() * 1000.0);

    // Report index size on disk
    let mut total_size = 0u64;
    for entry in std::fs::read_dir(index_dir)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            total_size += entry.metadata()?.len();
        }
    }
    println!("Index on disk: {}", human_size(total_size));

    Ok(index)
}

fn run_benchmark(index: &Index) {
    let test_queries = [
        "info", "exe", "a", "Windows", "zz", "System32", "bin", "tmp", ".dll",
    ];

    println!("\n--- Search benchmark ---");
    println!(
        "{:<12} {:>8} {:>12}",
        "query", "results", "time"
    );
    println!("{:-<12} {:-<8} {:-<12}", "", "", "");

    for query in &test_queries {
        let t0 = Instant::now();
        let results = index
            .search(query, 10_000_000, 0, SearchOptions::default())
            .unwrap();
        let elapsed = t0.elapsed();

        println!(
            "{:<12} {:>8} {:>12.3}ms",
            query,
            results.len(),
            elapsed.as_secs_f64() * 1000.0
        );
    }

    let multi_queries = [
        "Stro Chang",
        "esuprt disp",
        "Windows System32",
        "exe info",
        "bin tmp",
        "a Windows",
        ".dll System32",
    ];

    println!("\n--- Multi-token search benchmark ---");
    println!(
        "{:<24} {:>8} {:>12}",
        "query", "results", "time"
    );
    println!("{:-<24} {:-<8} {:-<12}", "", "", "");

    for query in &multi_queries {
        let t0 = Instant::now();
        let results = index
            .search(query, 10_000_000, 0, SearchOptions::default())
            .unwrap();
        let elapsed = t0.elapsed();

        println!(
            "{:<24} {:>8} {:>12.3}ms",
            query,
            results.len(),
            elapsed.as_secs_f64() * 1000.0
        );
    }
}

fn interactive_cli(index: &Index) {
    println!("\n=== Interactive search (type query, enter to search, 'q' to quit) ===");
    println!("  prefix with !N to show first N results (e.g. !20 info)\n");

    let stdin = stdin();
    loop {
        print!("> ");
        stdout().flush().unwrap();

        let mut line = String::new();
        if stdin.lock().read_line(&mut line).unwrap() == 0 {
            break;
        }
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if line == "q" || line == "quit" {
            break;
        }

        let mut show_count: usize = 10;
        let mut query = line;

        // Parse !N prefix
        if let Some(rest) = query.strip_prefix('!') {
            let num_end = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
            if num_end > 0 {
                if let Ok(n) = rest[..num_end].parse::<usize>() {
                    show_count = n;
                }
            }
            query = rest[num_end..].trim_start();
        }

        if query.is_empty() {
            continue;
        }

        let t0 = Instant::now();
        let results = match index.search(query, show_count, 0, SearchOptions::default()) {
            Ok(r) => r,
            Err(e) => {
                println!("  error: {}", e);
                continue;
            }
        };
        let elapsed = t0.elapsed();

        // Also get total count with large limit
        let total = index
            .search(query, 10_000_000, 0, SearchOptions::default())
            .map(|r| r.len())
            .unwrap_or(0);

        println!(
            "  {} results | search: {:.3}ms",
            total,
            elapsed.as_secs_f64() * 1000.0
        );

        for r in &results {
            println!("    {}", r.path.display());
        }
        if total > show_count {
            println!("    ... and {} more", total - show_count);
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    let mut file_path: Option<&str> = None;
    let mut interactive = false;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--interactive" | "-i" => interactive = true,
            _ => {
                if file_path.is_none() {
                    file_path = Some(&args[i]);
                }
            }
        }
        i += 1;
    }

    let file_path = match file_path {
        Some(p) => p,
        None => {
            eprintln!(
                "Usage: {} <everything_paths.txt> [--interactive | -i]",
                args[0]
            );
            std::process::exit(1);
        }
    };

    let index_dir = "minidex_bench_index";

    println!("Loading paths from: {}", file_path);
    let index = build_index(file_path, index_dir)?;

    print_process_memory();

    if interactive {
        interactive_cli(&index);
    } else {
        run_benchmark(&index);
        print_process_memory();
    }

    // Clean up index directory
    drop(index);
    let _ = std::fs::remove_dir_all(index_dir);

    Ok(())
}
