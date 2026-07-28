#[cfg(test)]
mod tests {
    use super::super::query_memory_budget::{estimate_value_bytes, QueryMemoryBudget};
    use surrealdb::sql::value;

    #[test]
    fn breach_is_detected() {
        let b = QueryMemoryBudget::new(8);
        let v = value!({ a: ["abcdef", "gh"] });
        let need = estimate_value_bytes(&v);
        assert!(need > 8);
        assert!(!b.try_consume(need));
    }

    #[test]
    fn unbounded_is_noop() {
        let b = QueryMemoryBudget::unbounded();
        assert!(b.try_consume(usize::MAX / 2));
    }
}
