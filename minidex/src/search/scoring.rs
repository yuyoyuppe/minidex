use crate::Kind;

/// Configurable weights for search result scoring.
#[derive(Debug)]
pub struct ScoringConfig {
    /// Token coverage ratio
    pub token_coverage: f64,
    /// Exact token match (not just prefix match)
    pub exact_match: f64,
    /// Query token matching in file name
    pub filename_match: f64,
    /// Penatly multiplier for path depth (applied as -weight * ln(depth))
    /// Surfaces shallower results first
    pub depth_penalty: f64,
    /// Maximum recency boost (decays logarithmically)
    pub recency_boost: f64,
    /// Directory boost (vs files).
    pub kind_dir_boost: f64,
    /// Boost by proximity scoring
    pub proximity_bonus: f64,
    /// Boost by token ordering
    pub ordering_bonus: f64,
}

impl Default for ScoringConfig {
    fn default() -> Self {
        Self {
            token_coverage: 30.0,
            exact_match: 10.0,
            filename_match: 15.0,
            depth_penalty: 2.0,
            recency_boost: 10.0,
            kind_dir_boost: 2.0,
            proximity_bonus: 20.0,
            ordering_bonus: 15.0,
        }
    }
}

pub(crate) fn compute_score(
    config: &ScoringConfig,
    path: &str,
    query_tokens: &[String],
    raw_query_tokens: &[String],
    last_modified: u64,
    kind: Kind,
    now_micros: f64,
) -> f64 {
    let path_tokens = crate::tokenizer::tokenize(path);
    let normalized = path.to_lowercase();

    let mut score = 0.0;

    // Calculate token coverage
    if !path_tokens.is_empty() {
        let matched = path_tokens
            .iter()
            .filter(|path_token| {
                query_tokens
                    .iter()
                    .any(|query_token| path_token.starts_with(query_token.as_str()))
            })
            .count();

        score += config.token_coverage * (matched as f64 / path_tokens.len() as f64);
    }

    // Exact token matches
    let exact = query_tokens
        .iter()
        .filter(|query_token| {
            path_tokens
                .iter()
                .any(|path_token| path_token == *query_token)
        })
        .count();

    score += config.exact_match * exact as f64;

    // Filename match: query tokens found in the last path component
    let filename_start = path
        .rfind(std::path::MAIN_SEPARATOR)
        .map(|i| i + 1)
        .unwrap_or(0);

    let filename_lower = &normalized[filename_start..];
    let filename_hits = query_tokens
        .iter()
        .filter(|query_token| filename_lower.contains(query_token.as_str()))
        .count();

    score += config.filename_match * filename_hits as f64;

    // Path depth penalty
    let depth = path
        .chars()
        .filter(|c| *c == std::path::MAIN_SEPARATOR)
        .count();
    if depth > 1 {
        score -= config.depth_penalty * (depth as f64).ln();
    }

    // Recency boost
    let age_days = (now_micros - last_modified as f64) / (1_000_000.0 * 86_400.0);
    if age_days > 0.0 {
        score += config.recency_boost / (1.0 + age_days.ln());
    } else {
        score += config.recency_boost
    }

    // Kind preference
    score += match kind {
        Kind::Directory => config.kind_dir_boost,
        Kind::File => config.kind_dir_boost * 0.5,
        Kind::Symlink => config.kind_dir_boost * 0.1,
    };

    // Continuous Position Proximity Scoring/Span Density
    // Calculates the bounding box of the matched tokens
    let mut min_pos = usize::MAX;
    let mut max_pos = 0;
    let mut total_token_len = 0;
    let mut matched_count = 0;

    for q in query_tokens {
        if let Some(pos) = normalized.find(q.as_str()) {
            min_pos = min_pos.min(pos);
            max_pos = max_pos.max(pos + q.len());
            total_token_len += q.len();
            matched_count += 1;
        }
    }

    // Only calculate proximity if multiple different tokens matched
    if matched_count > 1 && max_pos > min_pos {
        let span = max_pos - min_pos;

        // Density is the ratio of actual token characters to the span window size.
        // We use .min(1.0) because overlapping substring matches could technically exceed 1.0.
        let density = (total_token_len as f64 / span as f64).min(1.0);

        score += config.proximity_bonus * density;
    }

    // Hierarchical Ordering Bonus
    // Gives a bonus for tokens appearing in the exact sequence as the query
    if raw_query_tokens.len() > 1 {
        let mut last_pos = 0;
        let mut is_ordered = true;

        for raw_token in raw_query_tokens {
            // Search only the portion of the path that comes AFTER the previous token
            if let Some(pos) = normalized[last_pos..].find(raw_token.as_str()) {
                last_pos += pos + raw_token.len();
            } else {
                // If it's missing, or appears earlier in the string (out of order), we fail the bonus
                is_ordered = false;
                break;
            }
        }

        if is_ordered {
            score += config.ordering_bonus;
        }
    }

    score
}
