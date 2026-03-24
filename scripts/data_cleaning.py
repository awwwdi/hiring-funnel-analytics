import pandas as pd

# ======================
# LOAD DATA
# ======================

master = pd.read_csv('data/raw/master.csv')
naukri = pd.read_csv('data/raw/naukri.csv')
apna = pd.read_csv('data/raw/apna.csv')
ref1 = pd.read_csv('data/raw/referral_1.csv')
ref2 = pd.read_csv('data/raw/referral_2.csv')

# ======================
# CLEAN MASTER SHEET
# ======================

# Clean column names
master.columns = master.columns.str.strip()

# Convert dates
master['Lead Generation Date'] = pd.to_datetime(master['Lead Generation Date'], errors='coerce')
master['Interview Date'] = pd.to_datetime(master['Interview Date'], errors='coerce')

# Clean text columns safely
master['Call Status'] = master['Call Status'].astype(str).str.strip().str.lower()
master['Interview Status (Not Scheduled/ Scheduled/Completed)'] = master['Interview Status (Not Scheduled/ Scheduled/Completed)'].astype(str).str.strip().str.lower()
master['Selection status'] = master['Selection status'].astype(str).str.strip().str.lower()
master['Source'] = master['Source'].astype(str).str.strip()

# Standardize Source
master['Source'] = master['Source'].replace({
    'Online Portal (Naukri, Apna, LinkedIn)': 'Online',
    'Referral': 'Referral'
})

# ======================
# CREATE FUNNEL FLAGS (BULLETPROOF)
# ======================

master['is_connected'] = master['Call Status'] \
    .fillna('') \
    .astype(str) \
    .str.lower() \
    .apply(lambda x: 1 if x == 'connected' else 0)

master['is_interview'] = master['Interview Status (Not Scheduled/ Scheduled/Completed)'] \
    .fillna('') \
    .astype(str) \
    .str.lower() \
    .apply(lambda x: 1 if 'done' in x else 0)

master['is_pass'] = master['Selection status'] \
    .fillna('') \
    .astype(str) \
    .str.lower() \
    .apply(lambda x: 1 if x == 'pass' else 0)

# ======================
# SAVE CLEANED MASTER
# ======================

master.to_csv('data/processed/master_cleaned.csv', index=False)

# ======================
# CLEAN REFERRAL DATA
# ======================

# Clean column names
ref1.columns = ref1.columns.str.strip()
ref2.columns = ref2.columns.str.strip()

# Combine both referral sheets
referrals = pd.concat([ref1, ref2], ignore_index=True)

# Identify referral name columns dynamically
referral_cols = [col for col in referrals.columns if 'Referral' in col and 'Name' in col]

# Count number of referrals per row
referrals['total_referrals'] = referrals[referral_cols].notna().sum(axis=1)

# ======================
# SAVE CLEANED REFERRALS
# ======================

referrals.to_csv('data/processed/referrals_cleaned.csv', index=False)

# ======================
# SUMMARY OUTPUT
# ======================

summary = master.groupby('Source')[['is_connected', 'is_interview', 'is_pass']].sum()

print("\n=== FUNNEL SUMMARY ===")
print(summary)

print("\nReferral data processed successfully.")

# ======================
# WEEK FILTER (WTD)
# ======================

start_date = '2026-03-16'  # change this anytime

filtered = master[master['Lead Generation Date'] >= start_date]

# ======================
# TRACKER LOGIC
# ======================

tracker = filtered.groupby('Source')[['is_connected', 'is_interview', 'is_pass']].sum()

# Conversion %
tracker['conversion'] = (tracker['is_pass'] / tracker['is_connected']) * 100

print("\n=== WTD TRACKER ===")
print(tracker)

# ======================
# CLEAN TRACKER (ONLY MAIN SOURCES)
# ======================

main_tracker = tracker.loc[['Online', 'Referral']].copy()

# Add Total Row
total = main_tracker.sum()
total.name = 'Total'

main_tracker = pd.concat([main_tracker, total.to_frame().T])

print("\n=== FINAL TRACKER VIEW ===")
print(main_tracker)

# ======================
# EXTRA METRICS
# ======================

total_connected = main_tracker.loc['Total', 'is_connected']
total_interview = main_tracker.loc['Total', 'is_interview']
total_pass = main_tracker.loc['Total', 'is_pass']

pass_on_connects = (total_pass / total_connected) * 100 if total_connected != 0 else 0
pass_on_interviews = (total_pass / total_interview) * 100 if total_interview != 0 else 0

print("\n=== KEY METRICS ===")
print(f"Pass on Connects: {pass_on_connects:.2f}%")
print(f"Pass on Interviews: {pass_on_interviews:.2f}%")

# ======================
# SAVE CLEAN DATA FOR POWER BI (FINAL FIX)
# ======================

# Keep only needed rows (Online + Referral + Total already exists)
main_tracker_clean = main_tracker.copy()

# Reset index (VERY IMPORTANT)
main_tracker_clean = main_tracker_clean.reset_index()

# Drop ANY weird/empty columns if present
main_tracker_clean = main_tracker_clean.loc[:, main_tracker_clean.columns.notna()]

# Rename columns cleanly
main_tracker_clean.columns = ['Source', 'is_connected', 'is_interview', 'is_pass', 'conversion']

# Remove any completely empty column names just in case
main_tracker_clean = main_tracker_clean.loc[:, main_tracker_clean.columns != '']

# Save FINAL clean file
main_tracker_clean.to_csv('data/processed/final_tracker.csv', index=False)

# ======================
# WEEKLY TREND DATA
# ======================

# Create Week column
master['week'] = master['Lead Generation Date'].dt.to_period('W').astype(str)

# Group by week
weekly = master.groupby('week')[['is_connected', 'is_interview', 'is_pass']].sum().reset_index()

# Save it
weekly.to_csv('data/processed/weekly_trend.csv', index=False)

print("\nWeekly trend data saved.")