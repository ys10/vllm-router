use agent_router::{rollout_bucket, RolloutPercent, RoutingContext};
use uuid::Uuid;

#[test]
fn rollout_accepts_only_the_declared_stages() {
    for percent in [0, 1, 5, 25, 100] {
        assert_eq!(RolloutPercent::new(percent).unwrap().get(), percent);
    }
    for percent in [2, 10, 24, 26, 99, 101, u8::MAX] {
        assert!(RolloutPercent::new(percent).is_err());
    }
}

#[test]
fn rollout_buckets_have_exact_percentages_and_nested_membership() {
    let stages = [
        RolloutPercent::new(0).unwrap(),
        RolloutPercent::new(1).unwrap(),
        RolloutPercent::new(5).unwrap(),
        RolloutPercent::new(25).unwrap(),
        RolloutPercent::new(100).unwrap(),
    ];
    let ids: Vec<Uuid> = (0..100).map(Uuid::from_u128).collect();

    for stage in stages {
        assert_eq!(
            ids.iter().filter(|id| stage.includes(**id)).count(),
            stage.get() as usize
        );
    }
    for id in ids {
        let memberships: Vec<bool> = stages.iter().map(|stage| stage.includes(id)).collect();
        assert!(memberships.windows(2).all(|pair| !pair[0] || pair[1]));
    }
}

#[test]
fn routing_context_clones_keep_the_same_retry_bucket() {
    let context = RoutingContext::default();
    let retry_context = context.clone();

    assert_eq!(context.routing_id(), retry_context.routing_id());
    assert_eq!(
        rollout_bucket(context.routing_id()),
        rollout_bucket(retry_context.routing_id())
    );
}
