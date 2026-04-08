"""
Customer management layer for ZenTaxCore.

This package provides a simple convention for grouping multiple wallets
under a single customer directory.

Directory structure (per customer):

    taxtrack/customers/<customer_name>/
        config.json
        inbox/
        reports/

The `run_customer.py` runner in `taxtrack/root/` orchestrates processing
for these customers by reusing the existing tax engine pipeline.
"""

