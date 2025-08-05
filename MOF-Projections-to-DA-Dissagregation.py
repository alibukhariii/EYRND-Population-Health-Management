#!/usr/bin/env python
# coding: utf-8

# In[86]:


# Import required libraries
import pandas as pd
import numpy as np

# Age group mapping function for single-year age data
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

# === Step 1: Load data ===
xls = pd.ExcelFile('York_Durham_PHU_PopPorjections_2023-2051.xlsx')
df_proj = xls.parse('Projection_cleaned')
df_da = xls.parse('DA x Age x Sex')

# === Step 2: Rename columns for consistency ===
df_da.rename(columns={'Sum of Pop': 'Pop'}, inplace=True)

# === Step 3: Data Quality Checks - Required columns ===
required_proj_cols = ['YEAR (JULY 1)', 'REGION NAME', 'AgeGroup', 'Sex', 'Pop']
required_da_cols = ['DA_UID', 'AgeGroup', 'Sex', 'Pop']

assert all(col in df_proj.columns for col in required_proj_cols), "Missing columns in projection data"
assert all(col in df_da.columns for col in required_da_cols), "Missing columns in DA data"

# Drop rows with nulls in required columns
df_proj = df_proj.dropna(subset=required_proj_cols).copy()
df_da = df_da.dropna(subset=required_da_cols).copy()

# === Step 4: Clean and standardize Sex column ===
df_proj['Sex'] = df_proj['Sex'].replace({'W': 'F'})
df_proj = df_proj[df_proj['Sex'].isin(['M', 'F'])].copy()
df_da = df_da[df_da['Sex'].isin(['M', 'F'])].copy()


# In[87]:


# === Step 5: Handle mixed age group data in projection file ===
# Projection file may have single years or grouped ages.
# If single years exist, map them to custom groups and aggregate.
if df_proj['AgeGroup'].dtype == 'object' and df_proj['AgeGroup'].str.isnumeric().any():
    # Assume single years stored as strings - convert and map
    df_proj['AgeGroupNum'] = pd.to_numeric(df_proj['AgeGroup'], errors='coerce')
    # Map single years to custom groups where applicable
    df_proj.loc[df_proj['AgeGroupNum'].notnull(), 'AgeGroup'] = df_proj.loc[df_proj['AgeGroupNum'].notnull(), 'AgeGroupNum'].apply(map_single_year_to_custom_group)
    df_proj.drop(columns=['AgeGroupNum'], inplace=True)
    # Aggregate to custom groups
    df_proj = (
        df_proj.groupby(['YEAR (JULY 1)', 'REGION NAME', 'AgeGroup', 'Sex'], dropna=False)['Pop']
        .sum()
        .reset_index()
    )
    


# In[88]:


# Define target age groups
target_age_groups = ['0-17', '18-44', '45-64', '65-84', '85+']

# Filter projection data to keep only rows with valid age groups
df_proj = df_proj[df_proj['AgeGroup'].isin(target_age_groups)].copy()

# Proceed with aggregation to custom age groups if needed (from step 5)
df_proj = (
df_proj.groupby(['YEAR (JULY 1)', 'REGION NAME', 'AgeGroup', 'Sex'], dropna=False)['Pop']
.sum()
.reset_index()
)

print(df_proj.head())


# In[90]:


# Define DA prefixes or lists for York and Durham
# (Replace these with actual logic or a lookup if you have it)
durham_prefix = '3518'  # example prefix for Durham DAs
york_prefix = '3519'    # example prefix for York DAs

### Constants for DA border splitting
#DA_BORDER_DAs = {
 #   '35190009': {'Durham': 0.125, 'York': 0.875},
  #  '35190965': {'Durham': 0.028, 'York': 0.972}
#}


def assign_region_by_da(da_uid):
    if str(da_uid).startswith(durham_prefix):
        return 'Durham'
    elif str(da_uid).startswith(york_prefix):
        return 'York'
    else:
        return None  # outside scope

# Assign region based on DA_UID
df_da['REGION NAME'] = df_da['DA_UID'].apply(assign_region_by_da)

# Remove rows without assigned region
df_da = df_da[df_da['REGION NAME'].notnull()].copy()

# Check assignment results
print(df_da['REGION NAME'].value_counts())
print(df_da.head())


# In[91]:


# Step 1: Calculate baseline regional totals from DA data (2021)
baseline_region_totals = (
    df_da.groupby(['REGION NAME', 'AgeGroup', 'Sex'])['Pop']
    .sum()
    .reset_index()
    .rename(columns={'Pop': 'Baseline_Region_Pop'})
)

#Good


# In[92]:


# Merge baseline totals back to DA data
df_da = df_da.merge(baseline_region_totals, on=['REGION NAME', 'AgeGroup', 'Sex'])

#Good. Gets the Age x Sex x Region pop as a column for each DA


# In[93]:


#Step 2: Create a fixed lookup table for York and Durham CSD pop counts by age group and sex for 2021 from census
# Build as a DataFrame
region_age_sex_2021 = pd.DataFrame([
    # Durham
    ['Durham', '0-17', 'M', 64230],
    ['Durham', '0-17', 'F', 61275],
    ['Durham', '18-44', 'M', 112090],
    ['Durham', '18-44', 'F', 115440],
    ['Durham', '45-64', 'M', 92015],
    ['Durham', '45-64', 'F', 98190],
    ['Durham', '65-84', 'M', 44930],
    ['Durham', '65-84', 'F', 52505],
    ['Durham', '85+', 'M', 5085],
    ['Durham', '85+', 'F', 8560],
    
    # York
    ['York', '0-17', 'M', 137415],
    ['York', '0-17', 'F', 129640],
    ['York', '18-44', 'M', 178575],
    ['York', '18-44', 'F', 184805],
    ['York', '45-64', 'M', 164490],
    ['York', '45-64', 'F', 179230],
    ['York', '65-84', 'M', 82165],
    ['York', '65-84', 'F', 92990],
    ['York', '85+', 'M', 9590],
    ['York', '85+', 'F', 14435],
], columns=['REGION NAME', 'AgeGroup', 'Sex', 'True_Region_Pop_2021'])

# map DA file region names to match:
df_da['REGION NAME'] = df_da['REGION NAME'].replace({
    'Durham Region Health Department': 'Durham',
    'York Region Public Health Services': 'York'
})


# In[94]:


df_da = df_da.merge(
    region_age_sex_2021,
    on=['REGION NAME', 'AgeGroup', 'Sex'],
    how='left'
)


# In[96]:


# Step 2: Calculate DA proportions within each region-age-sex group
df_da['Prop_DA_in_Region_2021'] = df_da['Pop'] / df_da['True_Region_Pop_2021']

#Good. Gets the proportion of each DA x Age x Sex as a proportion of the region's pop of the same groups
#UPDATE: It is not a proportion of the region's pops, but of the sum of all the DAs (aka the catchment area's) pops


# In[97]:


print(df_da.head(10))


# In[98]:


# Step 3: Merge proportions with future regional projections
df_proj.rename(columns={'YEAR (JULY 1)': 'Year', 'REGION NAME': 'REGION NAME'}, inplace=True)
df_proj_filtered = df_proj[df_proj['AgeGroup'].isin(target_age_groups)].copy()



# In[99]:


print(df_proj_filtered.head(10))


# In[100]:


# Mapping long region names in projection data to short ones used in DA data
region_name_map = {
    'Durham Region Health Department': 'Durham',
    'York Region Public Health Services': 'York'
}

# Apply mapping to df_proj_filtered
df_proj_filtered['Region_Short'] = df_proj_filtered['REGION NAME'].map(region_name_map)

# Use the existing REGION NAME in df_da as Region_Short
df_da['Region_Short'] = df_da['REGION NAME']



# In[102]:


df_combined = df_proj_filtered.merge(
    df_da[['DA_UID', 'Region_Short', 'AgeGroup', 'Sex', 'Prop_DA_in_Region_2021']],
    left_on=['Region_Short', 'AgeGroup', 'Sex'],
    right_on=['Region_Short', 'AgeGroup', 'Sex'],
    how='left'
)


# In[103]:


print(df_combined.head())


# In[104]:


# Step 4: Calculate DA-level projections by scaling regional pop by DA proportions
df_combined['Projected_Pop'] = df_combined['Pop'] * df_combined['Prop_DA_in_Region_2021']


# In[105]:


print(df_combined.head(10))


# In[106]:


# Step 5: Validate totals
region_age_sex_year_totals2 = (
    df_combined.groupby(['Year', 'REGION NAME', 'AgeGroup', 'Sex'])['Projected_Pop']
    .sum()
    .reset_index()
)


# In[115]:


# Save or export the final projected dataset
df_combined.to_csv('Projected_Population_DA_Level_2023_2051.csv', index=False)

print("Projection completed successfully.")


# In[ ]:




