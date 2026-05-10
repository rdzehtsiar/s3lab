// SPDX-License-Identifier: Apache-2.0

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ServerState {
    pub data_dir: String,
}

impl ServerState {
    pub fn new(data_dir: impl Into<String>) -> Self {
        Self {
            data_dir: data_dir.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ServerState;

    #[test]
    fn new_stores_data_dir_without_normalizing() {
        let state = ServerState::new("./s3lab-data");

        assert_eq!(state.data_dir, "./s3lab-data");
    }

    #[test]
    fn state_can_be_cloned_without_changing_values() {
        let state = ServerState::new("data");

        assert_eq!(state.clone(), state);
    }
}
