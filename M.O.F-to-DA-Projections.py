#Date: Aug. 5, 2025
#Programmer: Ali Bukhari
#Purpose: 
    # The Ontario Ministry of Finance provides PHU-level population projects from 2023-2051. The regions of York and Durham are relevant to the EYRND catchment area.
    # This script disaggregate regional population projections (for York and Durham Regions) into smaller geographies (Dissemination Areas, or DAs) by Sex and Age Group, from 2023 to 2051.
    # It sccount for border-straddling DAs (only 2) and adjust populations accordingly.
    # Ensure that DA-level projected populations align with regional-level totals through proportional allocation.
    # Export a final dataset for DA-level projections for use in further analysis or reporting.
# Method:
    # Import data; Clean data and align the two files (DA x AgeGroup x Sex x Pop and Regional projections file) including in age groups
    # Assign regions to each DA, handling border DAs. Verify the pop totals remain consistent
    # Calcualte DA pop shares: For each (Region, AgeGroup, Sex) combo, calculate the total regional population and then each DA’s share of that total within the region.
    # Merge DA shares with regional projections
    # For each (Region, Year, AgeGroup, Sex), sum the DA-level projections and compare with regional-level projections.

#!/usr/bin/env python
# coding: utf-8

# In[29]:


# Step 0: Imports and Settings
import pandas as pd
import numpy as np

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 50)

# Constants
DA_BORDER_DAs = {
    '35190009': {'Durham': 0.125, 'York': 0.875},
    '35190965': {'Durham': 0.028, 'York': 0.972}
}

# Define the custom age groups mapping from single years
def map_single_year_to_custom_group(age):
    if 0 <= age <= 17:
        return '0-17'
    elif 18 <= age <= 44:
        return '18-44'
    elif 45 <= age <= 64:
        return '45-64'
    elif 65 <= age <= 84:
        return '65-84'
    elif age >= 85:
        return '85+'
    else:
        return 'Unknown'


# In[30]:


# Step 1: Load Data
xls = pd.ExcelFile('York_Durham_PHU_PopPorjections_2023-2051.xlsx')

df_proj = xls.parse('Projection_cleaned')
df_da = xls.parse('DA x Age x Sex')


# In[31]:


# Step 2: Rename columns for consistency
df_da.rename(columns={'Sum of Pop': 'Pop'}, inplace=True)


# In[32]:


# Step 3: Data Quality Check - Validate required columns
required_proj_cols = ['YEAR (JULY 1)', 'REGION NAME', 'AgeGroup', 'Sex', 'Pop']
required_da_cols = ['DA_UID', 'AgeGroup', 'Sex', 'Pop']

assert all(col in df_proj.columns for col in required_proj_cols), "Projection sheet missing required columns"
assert all(col in df_da.columns for col in required_da_cols), "DA sheet missing required columns"

# Check for nulls in key columns
assert not df_proj[required_proj_cols].isnull().any().any(), "Nulls found in Projection sheet key columns"
assert not df_da[required_da_cols].isnull().any().any(), "Nulls found in DA sheet key columns"


# In[33]:


# Step 4: Clean and standardize Sex column in both datasets
df_proj['Sex'] = df_proj['Sex'].replace({'W': 'F'})
df_proj = df_proj[df_proj['Sex'].isin(['M', 'F'])]  # Remove 'T' or others
df_da = df_da[df_da['Sex'].isin(['M', 'F'])]  # Filter to valid sex codes only


# In[34]:


# Step 5: Map single-year AgeGroup in projection to custom groups (0-17, 18-44, etc.)
# Assumes Projection_cleaned AgeGroup contains single years as integers or strings of single years
# We'll first convert AgeGroup to numeric where possible and drop invalid rows
def safe_age_to_int(x):
    try:
        return int(x)
    except:
        return np.nan

df_proj['Age_numeric'] = df_proj['AgeGroup'].apply(safe_age_to_int)
df_proj = df_proj.dropna(subset=['Age_numeric'])

df_proj['AgeGroup_custom'] = df_proj['Age_numeric'].apply(map_single_year_to_custom_group)

# Aggregate projection pop by Year, Region, Sex, AgeGroup_custom
df_proj_agg = df_proj.groupby(['YEAR (JULY 1)', 'REGION NAME', 'Sex', 'AgeGroup_custom'], as_index=False)['Pop'].sum()

# Step 6: Verify aggregation is correct
print("Projection data aggregation check (total population by year-region-sex):")
print(df_proj_agg.groupby(['YEAR (JULY 1)', 'REGION NAME', 'AgeGroup_custom','Sex'])['Pop'].sum().head(20))

# Step 7: Clean DA data AgeGroup to match 4 groups (0-17, 18-44, 45-64,)
valid_age_groups_da = ['0-17', '18-44', '45-64', '65-84', '85+']

print("Regional projection age groups:")
print(df_proj['AgeGroup_custom'].value_counts())


# In[35]:


# Step 8: Assign Region to each DA using first 4 digits of DA_UID
df_da['DA_prefix'] = df_da['DA_UID'].astype(str).str[:4]

def assign_region(da_uid, prefix):
    if da_uid in DA_BORDER_DAs:
        return 'Border'
    elif prefix == '3519':
        return 'York'
    elif prefix == '3518':
        return 'Durham'
    else:
        return 'Unknown'

df_da['Region'] = df_da.apply(lambda x: assign_region(str(x['DA_UID']), x['DA_prefix']), axis=1)

# Check for unassigned or unknown regions
unknown_regions = df_da[df_da['Region'] == 'Unknown']
if len(unknown_regions) > 0:
    print("Warning: Some DAs with Unknown region assignment:")
    print(unknown_regions['DA_UID'].unique())


# In[36]:


# Step 9: Handle border-straddling DAs by splitting population according to proportions
# Create two rows for each border DA with population split accordingly
df_border = df_da[df_da['Region'] == 'Border'].copy()

rows_split = []
for _, row in df_border.iterrows():
    da_uid = str(row['DA_UID'])
    for region, prop in DA_BORDER_DAs[da_uid].items():
        new_row = row.copy()
        new_row['Region'] = region
        new_row['Pop'] = row['Pop'] * prop
        rows_split.append(new_row)

df_border_split = pd.DataFrame(rows_split)

# Remove border DAs from original df_da and append split rows
df_da_no_border = df_da[df_da['Region'] != 'Border'].copy()
df_da_clean = pd.concat([df_da_no_border, df_border_split], ignore_index=True)


# In[37]:


# Step 10: Verify population sums remain consistent after splitting border DAs
orig_pop_by_da = df_da.groupby('DA_UID')['Pop'].sum()
new_pop_by_da = df_da_clean.groupby('DA_UID')['Pop'].sum()

pop_diff = (orig_pop_by_da - new_pop_by_da).abs().sum()
print(f"Total population difference after splitting border DAs: {pop_diff}")
assert pop_diff < 1e-5, "Population total changed after splitting border DAs!"


# In[38]:


# Step 11: Compute DA shares of population within each (Region, AgeGroup, Sex)

# Recalculate total population by Region, AgeGroup_proj, and Sex
pop_region_age_sex = df_da_clean.groupby(['Region', 'AgeGroup', 'Sex'])['Pop'].sum().reset_index()
pop_region_age_sex.rename(columns={'Pop': 'RegionPop'}, inplace=True)


# In[39]:


# Merge back to DA data
df_da_clean = df_da_clean.merge(pop_region_age_sex, on=['Region', 'AgeGroup', 'Sex'], how='left')

# Calculate DA shares
df_da_clean['DA_Share'] = df_da_clean['Pop'] / df_da_clean['RegionPop']


# In[40]:


print(df_da_clean.head())


# In[42]:


# Step 12: Data quality check for shares summing to 1 per (Region, AgeGroup, Sex)
shares_sum = df_da_clean.groupby(['Region', 'AgeGroup', 'Sex'])['DA_Share'].sum().reset_index()
shares_sum['WithinTolerance'] = np.isclose(shares_sum['DA_Share'], 1, atol=0.001)

if not shares_sum['WithinTolerance'].all():
    print("Warning: DA shares do not sum to 1 for some groups:")
    print(shares_sum[~shares_sum['WithinTolerance']])


# In[43]:


# Step 14: Merge DA shares with regional projections on Region, Sex, AgeGroup
# Rename Region to match naming in projection: 'York' and 'Durham' must match exactly
# Check if df_proj_agg REGION NAME uses exact strings 'Durham Region Health Department' and 'York Region Public Health Services'

# Map Region to projection region names
region_map_proj = {
    'York': 'York Region Public Health Services',
    'Durham': 'Durham Region Health Department'
}

df_da_clean['Region_proj_name'] = df_da_clean['Region'].map(region_map_proj)

# Now merge with df_proj_agg on Year, Region_proj_name, Sex, AgeGroup_custom

# We want projections for all years 2023-2041 (or full range in df_proj_agg)
years = df_proj_agg['YEAR (JULY 1)'].unique()

# Prepare to create final projected pop per DA x Year x Sex x AgeGroup
result_rows = []

for year in years:
    df_proj_year = df_proj_agg[df_proj_agg['YEAR (JULY 1)'] == year]
    
    # Merge DA shares with projection pop
    df_merged = df_da_clean.merge(df_proj_year,
                                 left_on=['Region_proj_name', 'Sex', 'AgeGroup'],
                                 right_on=['REGION NAME', 'Sex', 'AgeGroup_custom'],
                                 how='left')
    
    # Calculate projected population per DA
    df_merged['Projected_Pop'] = df_merged['DA_Share'] * df_merged['Pop_y']  # Pop_y = regional projected pop
    
    # Collect relevant columns
    df_year = df_merged[['DA_UID', 'Sex', 'AgeGroup', 'Region', 'Region_proj_name', 'YEAR (JULY 1)', 'Projected_Pop']].copy()
    
    result_rows.append(df_year)

# Concatenate all years
df_projected = pd.concat(result_rows, ignore_index=True)


# In[46]:


# Step 15: Final Data Quality Check
# Sum projected pop by region, year, sex, age group and compare to regional projection totals
proj_check = df_projected.groupby(['YEAR (JULY 1)', 'Region_proj_name', 'Sex', 'AgeGroup'])['Projected_Pop'].sum().reset_index()
proj_check = proj_check.merge(df_proj_agg, left_on=['YEAR (JULY 1)', 'Region_proj_name', 'Sex', 'AgeGroup'],
                              right_on=['YEAR (JULY 1)', 'REGION NAME', 'Sex', 'AgeGroup_custom'],
                              how='left')

proj_check['Difference'] = proj_check['Projected_Pop'] - proj_check['Pop']

print("Summary of differences between summed DA projections and regional projections:")
print(proj_check[['YEAR (JULY 1)', 'Region_proj_name', 'Sex', 'AgeGroup', 'Difference']].head())

max_diff = proj_check['Difference'].abs().max()
print(f"Max difference (absolute) between projected DA sums and regional totals: {max_diff}")

assert max_diff < 1e-2, "Large differences found - check calculations."

# Step 16: Save or export the final projected dataset
df_projected.to_csv('Projected_Population_DA_Level_2023_2051.csv', index=False)

print("Projection completed successfully.")


# In[ ]:




