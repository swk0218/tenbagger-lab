from tech_pipeline.rules import evaluate_gate, load_rules


def test_load_rules():
    spec = load_rules("spec/quick_rules.md")
    assert spec.step_a
    assert "rs126" in spec.step_b


def test_evaluate_gate():
    assert evaluate_gate(10, ">=", 5)
    assert not evaluate_gate(1, ">=", 5)
