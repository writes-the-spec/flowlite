use std::sync::Arc;
use crate::toolkit::Toolkit;


#[derive(Clone)]
pub struct CRUD {
    pub toolkit: Arc<Toolkit>,
}


impl CRUD {

    pub fn new(
        toolkit: Arc<Toolkit>,
    ) -> Self {
        Self { toolkit }
    }
}
