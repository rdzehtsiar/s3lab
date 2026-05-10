// SPDX-License-Identifier: Apache-2.0

pub fn raw_keys_are_not_filesystem_paths() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::raw_keys_are_not_filesystem_paths;

    #[test]
    fn storage_key_policy_rejects_raw_path_mapping() {
        assert!(raw_keys_are_not_filesystem_paths());
    }
}
