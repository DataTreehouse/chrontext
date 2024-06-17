#[derive(PartialEq, Debug, Clone)]
pub enum ChangeType {
    Relaxed,
    Constrained,
    NoChange,
}

impl ChangeType {
    pub fn opposite(&self) -> ChangeType {
        match self {
            ChangeType::Relaxed => ChangeType::Constrained,
            ChangeType::Constrained => ChangeType::Relaxed,
            ChangeType::NoChange => ChangeType::NoChange,
        }
    }
}
