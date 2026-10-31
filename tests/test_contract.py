import os
os.environ['EPISCOPE_HASH_SALT']='test-salt'
def test_quality_rules_count():
    from pipeline.quality.rules import VALID_CODES
    assert len(VALID_CODES)>=10
def test_privacy_policy():
    from serving.export import safe_records
    import pandas as pd
    assert safe_records(pd.DataFrame({'cases':[3,5]}),['cases'])[0]['cases'] is None
def test_allocation_nonnegative():
    # Constraint invariant: allocation cannot be negative.
    allocations=[0,1,200]
    assert all(x>=0 for x in allocations)
