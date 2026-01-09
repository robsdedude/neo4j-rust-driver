#[derive(Debug, Clone)]
pub struct UnsupportedType {
    pub(crate) name: String,
    pub(crate) minimum_protocol_version: (u8, u8),
    pub(crate) message: Option<String>,
}

impl UnsupportedType {
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn minimum_protocol_version(&self) -> (u8, u8) {
        self.minimum_protocol_version
    }

    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }
}

impl PartialEq for UnsupportedType {
    fn eq(&self, _: &Self) -> bool {
        false
    }
}
