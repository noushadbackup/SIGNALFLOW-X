-- Type 1 (current snapshot). Dealer master data has no change history in Phase 1; SCD2 is added when the ERP
-- CDC feed (Phase 2) provides dealer changes.
select dealer_id, dealer_name, region_code, tier, credit_days from {{ source('ref', 'dealers') }}
