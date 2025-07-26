use std::usize;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreviousStage {
    #[prost(string, tag = "1")]
    pub id: String,
    #[prost(uint64, tag = "2")]
    pub caller: u64,
    #[prost(string, repeated, tag = "3")]
    pub actors: Vec<String>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StageContext {
    #[prost(string, tag = "1")]
    pub id: String,
    #[prost(message, optional, tag = "2")]
    pub previous_stage: Option<PreviousStage>,
    #[prost(uint64, tag = "3")]
    pub current: u64,
    #[prost(uint64, tag = "4")]
    pub delegate: u64,
    #[prost(string, repeated, tag = "5")]
    pub actors: Vec<String>,
}

impl StageContext {
    pub fn to_previous(&self) -> PreviousStage {
        PreviousStage {
            id: self.id.clone(),
            caller: self.current,
            actors: self.actors.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stage_context_creation() {
        let context = StageContext {
            id: "stage-123".to_string(),
            previous_stage: Some(PreviousStage {
                id: "prev-stage-456".to_string(),
                caller: 5,
                actors: vec![
                    "http://localhost:8079".to_string(),
                    "http://localhost:8078".to_string(),
                ],
            }),
            current: 0,
            delegate: 1,
            actors: vec![
                "http://localhost:8080".to_string(),
                "http://localhost:8081".to_string(),
            ],
        };

        assert_eq!(context.id, "stage-123");
        assert_eq!(context.current, 0);
        assert_eq!(context.delegate, 1);
        assert_eq!(context.actors.len(), 2);
        assert!(context.previous_stage.is_some());

        if let Some(prev) = &context.previous_stage {
            assert_eq!(prev.id, "prev-stage-456");
            assert_eq!(prev.caller, 5);
            assert_eq!(prev.actors.len(), 2);
        }
    }

    #[test]
    fn test_stage_context_without_previous() {
        let context = StageContext {
            id: "stage-789".to_string(),
            previous_stage: None,
            current: 2,
            delegate: 3,
            actors: vec!["http://localhost:9000".to_string()],
        };

        assert_eq!(context.id, "stage-789");
        assert_eq!(context.current, 2);
        assert_eq!(context.delegate, 3);
        assert_eq!(context.actors.len(), 1);
        assert!(context.previous_stage.is_none());
    }

    #[test]
    fn test_empty_actors() {
        let context = StageContext {
            id: "empty-stage".to_string(),
            previous_stage: None,
            current: 5,
            delegate: 10,
            actors: vec![],
        };

        assert_eq!(context.id, "empty-stage");
        assert_eq!(context.current, 5);
        assert_eq!(context.delegate, 10);
        assert!(context.actors.is_empty());
        assert!(context.previous_stage.is_none());
    }

    #[test]
    fn test_to_previous() {
        let context = StageContext {
            id: "current-stage".to_string(),
            previous_stage: None,
            current: 5,
            delegate: 10,
            actors: vec![
                "http://localhost:8080".to_string(),
                "http://localhost:8081".to_string(),
            ],
        };

        let previous = context.to_previous();
        assert_eq!(previous.id, context.id);
        assert_eq!(previous.caller, context.current);
        assert_eq!(previous.actors, context.actors);
    }

    #[test]
    fn test_clone() {
        let context = StageContext {
            id: "clone-test".to_string(),
            previous_stage: Some(PreviousStage {
                id: "prev-clone".to_string(),
                caller: 1,
                actors: vec!["http://localhost:8000".to_string()],
            }),
            current: 2,
            delegate: 3,
            actors: vec!["http://localhost:8001".to_string()],
        };

        let cloned = context.clone();
        assert_eq!(context, cloned);
        assert_eq!(context.id, cloned.id);
        assert_eq!(context.current, cloned.current);
        assert_eq!(context.delegate, cloned.delegate);
        assert_eq!(context.actors, cloned.actors);
        assert_eq!(context.previous_stage, cloned.previous_stage);
    }

    #[test]
    fn test_previous_stage_creation() {
        let prev_stage = PreviousStage {
            id: "prev-123".to_string(),
            caller: 42,
            actors: vec![
                "http://example.com:8080".to_string(),
                "http://example.com:8081".to_string(),
                "http://example.com:8082".to_string(),
            ],
        };

        assert_eq!(prev_stage.id, "prev-123");
        assert_eq!(prev_stage.caller, 42);
        assert_eq!(prev_stage.actors.len(), 3);
        assert!(prev_stage
            .actors
            .contains(&"http://example.com:8080".to_string()));
    }

    #[test]
    fn test_partial_eq() {
        let context1 = StageContext {
            id: "test-stage".to_string(),
            previous_stage: None,
            current: 1,
            delegate: 2,
            actors: vec!["http://localhost:8080".to_string()],
        };

        let context2 = StageContext {
            id: "test-stage".to_string(),
            previous_stage: None,
            current: 1,
            delegate: 2,
            actors: vec!["http://localhost:8080".to_string()],
        };

        let context3 = StageContext {
            id: "different-stage".to_string(),
            previous_stage: None,
            current: 1,
            delegate: 2,
            actors: vec!["http://localhost:8080".to_string()],
        };

        assert_eq!(context1, context2);
        assert_ne!(context1, context3);
    }

    #[test]
    fn test_prost_serialization() {
        use prost::Message;

        let context = StageContext {
            id: "prost-test".to_string(),
            previous_stage: Some(PreviousStage {
                id: "prev-prost".to_string(),
                caller: 99,
                actors: vec!["http://test.com:8080".to_string()],
            }),
            current: 10,
            delegate: 20,
            actors: vec![
                "http://actor1.com:8080".to_string(),
                "http://actor2.com:8080".to_string(),
            ],
        };

        // Serialize to bytes
        let mut buf = Vec::new();
        context.encode(&mut buf).unwrap();

        // Deserialize back
        let decoded = StageContext::decode(&buf[..]).unwrap();

        assert_eq!(context, decoded);
        assert_eq!(context.id, decoded.id);
        assert_eq!(context.current, decoded.current);
        assert_eq!(context.delegate, decoded.delegate);
        assert_eq!(context.actors, decoded.actors);
        assert_eq!(context.previous_stage, decoded.previous_stage);
    }
}
