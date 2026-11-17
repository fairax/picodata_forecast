// All callers run on the Tarantool thread, including fibreq's yielding fibers.
#[allow(clippy::too_many_arguments)]
pub fn event(
    level: &str,
    event: &str,
    request_id: &str,
    operation: &str,
    outcome: &str,
    attempt: u64,
    status: u16,
    duration: f64,
    bucket_id: u64,
    detail: &str,
) {
    let _ = tarantool::lua_state().exec_with(
        "require('app.observability').native_event(...)",
        (
            level, event, request_id, operation, outcome, attempt, status, duration, bucket_id,
            detail,
        ),
    );
}
