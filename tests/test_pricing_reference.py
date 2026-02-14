from prop_ev.pricing_reference import (
    ReferencePoint,
    build_reference_curve,
    estimate_reference_probability,
)


def test_build_reference_curve_enforces_monotone_probability() -> None:
    points = [
        ReferencePoint(point=20.5, p_over=0.56, hold=0.05, weight=2.0),
        ReferencePoint(point=21.5, p_over=0.60, hold=0.05, weight=2.0),
        ReferencePoint(point=22.5, p_over=0.52, hold=0.04, weight=2.0),
    ]

    curve = build_reference_curve(points)

    assert len(curve) == 3
    assert curve[0].p_over >= curve[1].p_over >= curve[2].p_over


def test_estimate_reference_probability_interpolates_between_points() -> None:
    points = [
        ReferencePoint(point=20.5, p_over=0.58, hold=0.055, weight=2.0),
        ReferencePoint(point=24.5, p_over=0.46, hold=0.06, weight=2.0),
    ]

    estimate = estimate_reference_probability(points, target_point=22.5)

    assert estimate.method == "interpolated"
    assert estimate.points_used == 2
    assert estimate.p_over is not None
    assert 0.50 < estimate.p_over < 0.55
    assert estimate.hold is not None


def test_estimate_reference_probability_clamps_outside_range() -> None:
    points = [
        ReferencePoint(point=21.5, p_over=0.57, hold=0.05, weight=1.0),
        ReferencePoint(point=23.5, p_over=0.49, hold=0.05, weight=1.0),
    ]

    low = estimate_reference_probability(points, target_point=19.5)
    high = estimate_reference_probability(points, target_point=25.5)

    assert low.method == "clamped_low"
    assert high.method == "clamped_high"
    assert low.p_over is not None and high.p_over is not None
    assert low.p_over > high.p_over
