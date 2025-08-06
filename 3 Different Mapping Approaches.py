#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Ontario Marginalization Index DA to FSA Analysis
# Three approaches: DA counts, dominant FSA population, and proportional allocation

import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

print("="*60)
print("ONTARIO MARGINALIZATION INDEX DA TO FSA ANALYSIS")
print("="*60)

# =============================================================================
# STEP 1: DATA LOADING
# =============================================================================
print("\nSTEP 1: Loading all data files...")

# Load ON-MARG data from .xlsm file
print("Loading ON-MARG data...")
pccf_file = "EAVC Tool w PCCF OnMargIndex v4.xlsm"
onmarg_df = pd.read_excel(pccf_file, sheet_name='OnMarg2021')
print(f"  - ON-MARG data loaded: {onmarg_df.shape}")

# Load DA-FSA mapping from .csv file
print("Loading DA-FSA mapping...")
da_fsa_file = "DA_FSA_Proportions.csv"
da_fsa_df = pd.read_csv(da_fsa_file)
print(f"  - DA-FSA mapping loaded: {da_fsa_df.shape}")

# Load population data from data.csv
print("Loading population data...")
pop_file = "data.csv"
pop_df = pd.read_csv(pop_file)
print(f"  - Population data loaded: {pop_df.shape}")

# Display column names for verification
print("\nColumn verification:")
print("ON-MARG columns:", list(onmarg_df.columns))
print("DA-FSA columns:", list(da_fsa_df.columns))
print("Population columns:", list(pop_df.columns))


# In[2]:


# =============================================================================
# STEP 2: DATA PREPARATION AND VALIDATION
# =============================================================================
print("\nSTEP 2: Data preparation and validation...")

# Standardize DA ID columns to string type for consistent merging
onmarg_df['DAUID'] = onmarg_df['DAUID'].astype(str)
da_fsa_df['DAuid'] = da_fsa_df['DAuid'].astype(str)
pop_df['DA_UID'] = pop_df['DA_UID'].astype(str)

# Check for missing values
print("\nMissing values check:")
print(f"  - ON-MARG missing DAUIDs: {onmarg_df['DAUID'].isna().sum()}")
print(f"  - DA-FSA missing DAuids: {da_fsa_df['DAuid'].isna().sum()}")
print(f"  - Population missing DA_UIDs: {pop_df['DA_UID'].isna().sum()}")
print(f"  - Population missing Sum of Pop: {pop_df['Sum of Pop'].isna().sum()}")

# Check unique DA counts
print("\nUnique DA counts:")
print(f"  - ON-MARG unique DAs: {onmarg_df['DAUID'].nunique():,}")
print(f"  - DA-FSA unique DAs: {da_fsa_df['DAuid'].nunique():,}")
print(f"  - Population unique DAs: {pop_df['DA_UID'].nunique():,}")


# In[3]:


# =============================================================================
# STEP 3: MERGE ON-MARG DATA WITH POPULATION DATA
# =============================================================================
print("\nSTEP 3: Merging ON-MARG data with population data...")

# Merge ON-MARG data with population data
onmarg_with_pop = onmarg_df.merge(
    pop_df[['DA_UID', 'Sum of Pop']], 
    left_on='DAUID', 
    right_on='DA_UID', 
    how='left'
)

print(f"  - Merged dataset shape: {onmarg_with_pop.shape}")
print(f"  - DAs with missing population data: {onmarg_with_pop['Sum of Pop'].isna().sum()}")

# Remove DAs without population data
onmarg_with_pop = onmarg_with_pop.dropna(subset=['Sum of Pop'])
print(f"  - Final dataset after removing missing population: {onmarg_with_pop.shape}")

# Validation: Check total population
total_pop_original = pop_df['Sum of Pop'].sum()
total_pop_merged = onmarg_with_pop['Sum of Pop'].sum()
print(f"\nPopulation validation:")
print(f"  - Original total population: {total_pop_original:,}")
print(f"  - Merged total population: {total_pop_merged:,}")
print(f"  - Population match: {abs(total_pop_original - total_pop_merged) < 1000}")


# In[4]:


# =============================================================================
# STEP 4: ASSIGN DAs TO FSAs USING DOMINANT FSA
# =============================================================================
print("\nSTEP 4: Assigning DAs to FSAs using DominantFSA...")

# Get unique DA to dominant FSA mapping
da_to_dominant_fsa = da_fsa_df[['DAuid', 'DominantFSA']].drop_duplicates()
print(f"  - Unique DA to FSA mappings: {len(da_to_dominant_fsa)}")

# Merge with our main dataset
final_df = onmarg_with_pop.merge(
    da_to_dominant_fsa,
    left_on='DAUID',
    right_on='DAuid',
    how='left'
)

print(f"  - Final dataset shape: {final_df.shape}")
print(f"  - DAs without FSA assignment: {final_df['DominantFSA'].isna().sum()}")

# Remove DAs without FSA assignment
final_df = final_df.dropna(subset=['DominantFSA'])
print(f"  - Final dataset after removing unassigned DAs: {final_df.shape}")

# Validation: Check FSA assignments
print(f"\nFSA assignment validation:")
print(f"  - Number of unique FSAs: {final_df['DominantFSA'].nunique()}")
print(f"  - DAs per FSA (mean): {final_df.groupby('DominantFSA').size().mean():.1f}")
print(f"  - DAs per FSA (median): {final_df.groupby('DominantFSA').size().median():.1f}")


# In[5]:


# =============================================================================
# STEP 5: DEFINE DIMENSIONS AND PREPARE FOR ANALYSIS
# =============================================================================
print("\nSTEP 5: Setting up dimensions for analysis...")

# Define the four ON-MARG dimensions
dimensions = [
    'households_dwellings_q_DA21',
    'material_resources_q_DA21', 
    'age_labourforce_q_DA21',
    'racialized_NC_pop_q_DA21'
]

dimension_names = [
    'Residential_Instability',
    'Material_Deprivation',
    'Dependency',
    'Ethnic_Concentration'
]

print(f"  - Analyzing {len(dimensions)} dimensions:")
for i, dim_name in enumerate(dimension_names):
    print(f"    {i+1}. {dim_name}")

# Check quintile distributions
print("\nQuintile distribution check:")
for i, dim_col in enumerate(dimensions):
    quintile_counts = final_df[dim_col].value_counts().sort_index()
    print(f"  - {dimension_names[i]}: {dict(quintile_counts)}")


# In[6]:


# =============================================================================
# STEP 6: APPROACH 1 - DA COUNTS BY QUINTILE
# =============================================================================
print("\n" + "="*60)
print("APPROACH 1: DA COUNTS BY QUINTILE AND PROPORTION")
print("="*60)

def calculate_da_counts_approach(df, dimensions, dimension_names):
    """Calculate DA counts and proportions by FSA and quintile"""
    results = {}
    
    for i, dim_col in enumerate(dimensions):
        dim_name = dimension_names[i]
        print(f"\nProcessing {dim_name}...")
        
        # Count DAs by FSA and quintile
        da_counts = df.groupby(['DominantFSA', dim_col]).size().reset_index(name='DA_Count')
        da_counts = da_counts.rename(columns={dim_col: 'Quintile'})
        
        # Calculate total DAs per FSA
        fsa_totals = df.groupby('DominantFSA').size().reset_index(name='Total_DAs')
        
        # Merge and calculate proportions
        da_analysis = da_counts.merge(fsa_totals, on='DominantFSA')
        da_analysis['DA_Proportion'] = da_analysis['DA_Count'] / da_analysis['Total_DAs']
        da_analysis['DA_Percentage'] = da_analysis['DA_Proportion'] * 100
        
        results[dim_name] = da_analysis
        print(f"  - Completed: {len(da_analysis)} FSA-quintile combinations")
    
    return results

# Run Approach 1
approach1_results = calculate_da_counts_approach(final_df, dimensions, dimension_names)

# Validation for Approach 1
print("\nApproach 1 Validation:")
for dim_name in dimension_names:
    result = approach1_results[dim_name]
    total_das_check = result.groupby('DominantFSA')['DA_Count'].sum()
    total_das_original = result.groupby('DominantFSA')['Total_DAs'].first()
    matches = (total_das_check == total_das_original).all()
    print(f"  - {dim_name}: DA count validation passed: {matches}")


# In[7]:


# =============================================================================
# STEP 7: APPROACH 2 - DOMINANT FSA POPULATION METHOD
# =============================================================================
print("\n" + "="*60)
print("APPROACH 2: DOMINANT FSA POPULATION PERCENTAGES")
print("="*60)

def calculate_dominant_pop_approach(df, dimensions, dimension_names):
    """Calculate population percentages using dominant FSA only"""
    results = {}
    
    for i, dim_col in enumerate(dimensions):
        dim_name = dimension_names[i]
        print(f"\nProcessing {dim_name}...")
        
        # Sum population by FSA and quintile
        pop_by_quintile = df.groupby(['DominantFSA', dim_col])['Sum of Pop'].sum().reset_index()
        pop_by_quintile = pop_by_quintile.rename(columns={dim_col: 'Quintile'})
        
        # Calculate total population per FSA
        fsa_pop_totals = df.groupby('DominantFSA')['Sum of Pop'].sum().reset_index()
        fsa_pop_totals = fsa_pop_totals.rename(columns={'Sum of Pop': 'Total_Population'})
        
        # Merge and calculate percentages
        pop_analysis = pop_by_quintile.merge(fsa_pop_totals, on='DominantFSA')
        pop_analysis['Pop_Percentage'] = (pop_analysis['Sum of Pop'] / pop_analysis['Total_Population']) * 100
        
        results[dim_name] = pop_analysis
        print(f"  - Completed: {len(pop_analysis)} FSA-quintile combinations")
    
    return results

# Run Approach 2
approach2_results = calculate_dominant_pop_approach(final_df, dimensions, dimension_names)

# Validation for Approach 2
print("\nApproach 2 Validation:")
for dim_name in dimension_names:
    result = approach2_results[dim_name]
    # Check if percentages sum to ~100% for each FSA
    pct_sums = result.groupby('DominantFSA')['Pop_Percentage'].sum()
    avg_sum = pct_sums.mean()
    print(f"  - {dim_name}: Average percentage sum per FSA: {avg_sum:.1f}% (should be ~100%)")


# In[8]:


# =============================================================================
# STEP 8: APPROACH 3 - PROPORTIONAL ALLOCATION METHOD
# =============================================================================
print("\n" + "="*60)
print("APPROACH 3: PROPORTIONAL ALLOCATION ACROSS FSAs")
print("="*60)

def calculate_proportional_approach(onmarg_df, pop_df, da_fsa_df, dimensions, dimension_names):
    """Calculate population percentages using proportional allocation"""
    
    print("Setting up proportional allocation...")
    
    # Merge all data together for proportional calculation
    # First merge onmarg with population
    base_df = onmarg_df.merge(
        pop_df[['DA_UID', 'Sum of Pop']], 
        left_on='DAUID', 
        right_on='DA_UID', 
        how='inner'
    )
    
    # Then merge with all DA-FSA proportions (not just dominant)
    prop_df = base_df.merge(
        da_fsa_df,
        left_on='DAUID',
        right_on='DAuid',
        how='inner'
    )
    
    # Calculate allocated population for each DA-FSA combination
    prop_df['Allocated_Population'] = prop_df['Sum of Pop'] * prop_df['Proportion']
    
    print(f"  - Proportional dataset shape: {prop_df.shape}")
    print(f"  - Original population: {pop_df['Sum of Pop'].sum():,}")
    print(f"  - Allocated population: {prop_df['Allocated_Population'].sum():,}")
    
    results = {}
    
    for i, dim_col in enumerate(dimensions):
        dim_name = dimension_names[i]
        print(f"\nProcessing {dim_name}...")
        
        # Sum allocated population by FSA and quintile
        pop_by_quintile = prop_df.groupby(['FSA', dim_col])['Allocated_Population'].sum().reset_index()
        pop_by_quintile = pop_by_quintile.rename(columns={dim_col: 'Quintile'})
        
        # Calculate total allocated population per FSA
        fsa_pop_totals = prop_df.groupby('FSA')['Allocated_Population'].sum().reset_index()
        fsa_pop_totals = fsa_pop_totals.rename(columns={'Allocated_Population': 'Total_Population'})
        
        # Merge and calculate percentages
        pop_analysis = pop_by_quintile.merge(fsa_pop_totals, on='FSA')
        pop_analysis['Pop_Percentage'] = (pop_analysis['Allocated_Population'] / pop_analysis['Total_Population']) * 100
        
        results[dim_name] = pop_analysis
        print(f"  - Completed: {len(pop_analysis)} FSA-quintile combinations")
    
    return results, prop_df

# Run Approach 3
approach3_results, proportional_df = calculate_proportional_approach(
    onmarg_df, pop_df, da_fsa_df, dimensions, dimension_names
)

# Validation for Approach 3
print("\nApproach 3 Validation:")
for dim_name in dimension_names:
    result = approach3_results[dim_name]
    # Check if percentages sum to ~100% for each FSA
    pct_sums = result.groupby('FSA')['Pop_Percentage'].sum()
    avg_sum = pct_sums.mean()
    print(f"  - {dim_name}: Average percentage sum per FSA: {avg_sum:.1f}% (should be ~100%)")


# In[9]:


# =============================================================================
# STEP 9: EXPORT RESULTS
# =============================================================================
print("\n" + "="*60)
print("EXPORTING RESULTS")
print("="*60)

# Export Approach 1 results
print("\nExporting Approach 1 (DA Counts)...")
with pd.ExcelWriter('Approach1_DA_Counts.xlsx', engine='openpyxl') as writer:
    for dim_name in dimension_names:
        approach1_results[dim_name].to_excel(writer, sheet_name=dim_name, index=False)
print("  - Exported: Approach1_DA_Counts.xlsx")

# Export Approach 2 results
print("\nExporting Approach 2 (Dominant FSA Population)...")
with pd.ExcelWriter('Approach2_Dominant_FSA_Population.xlsx', engine='openpyxl') as writer:
    for dim_name in dimension_names:
        approach2_results[dim_name].to_excel(writer, sheet_name=dim_name, index=False)
print("  - Exported: Approach2_Dominant_FSA_Population.xlsx")

# Export Approach 3 results
print("\nExporting Approach 3 (Proportional Allocation)...")
with pd.ExcelWriter('Approach3_Proportional_Allocation.xlsx', engine='openpyxl') as writer:
    for dim_name in dimension_names:
        approach3_results[dim_name].to_excel(writer, sheet_name=dim_name, index=False)
print("  - Exported: Approach3_Proportional_Allocation.xlsx")

# Create summary comparison
print("\nCreating summary comparison...")

# Function to create pivot tables for comparison
def create_summary_pivot(results_dict, dim_name, value_col, approach_name):
    """Create pivot table for summary comparison"""
    data = results_dict[dim_name]
    fsa_col = 'DominantFSA' if 'DominantFSA' in data.columns else 'FSA'
    
    pivot = data.pivot(index=fsa_col, columns='Quintile', values=value_col)
    pivot = pivot.fillna(0)
    pivot.columns = [f'Q{int(col)}_{approach_name}' for col in pivot.columns]
    return pivot

# Create comparison for first dimension as example
dim_example = dimension_names[0]
print(f"\nCreating comparison example for {dim_example}...")

approach1_pivot = create_summary_pivot(approach1_results, dim_example, 'DA_Percentage', 'A1_DA_Pct')
approach2_pivot = create_summary_pivot(approach2_results, dim_example, 'Pop_Percentage', 'A2_Dom_Pop_Pct')
approach3_pivot = create_summary_pivot(approach3_results, dim_example, 'Pop_Percentage', 'A3_Prop_Pop_Pct')

# Combine for comparison
comparison_df = approach1_pivot.join(approach2_pivot, how='outer').join(approach3_pivot, how='outer')
comparison_df = comparison_df.fillna(0).reset_index()

comparison_df.to_csv(f'{dim_example}_Three_Approaches_Comparison.csv', index=False)
print(f"  - Exported: {dim_example}_Three_Approaches_Comparison.csv")

# =============================================================================
# STEP 10: FINAL SUMMARY
# =============================================================================
print("\n" + "="*60)
print("ANALYSIS SUMMARY")
print("="*60)

print(f"\nDatasets processed:")
print(f"  - Total DAs in analysis: {len(final_df):,}")
print(f"  - Total FSAs (Approaches 1&2): {final_df['DominantFSA'].nunique()}")
print(f"  - Total FSAs (Approach 3): {proportional_df['FSA'].nunique()}")
print(f"  - Total population: {final_df['Sum of Pop'].sum():,}")

print(f"\nApproach 1 - DA Counts:")
print(f"  - Method: Count and percentage of DAs by quintile")
print(f"  - FSA Assignment: Dominant FSA only")

print(f"\nApproach 2 - Dominant FSA Population:")
print(f"  - Method: Population percentage by quintile")
print(f"  - FSA Assignment: Dominant FSA only")

print(f"\nApproach 3 - Proportional Allocation:")
print(f"  - Method: Population percentage by quintile")
print(f"  - FSA Assignment: Proportional across all FSAs")

print(f"\nFiles created:")
print(f"  1. Approach1_DA_Counts.xlsx")
print(f"  2. Approach2_Dominant_FSA_Population.xlsx")
print(f"  3. Approach3_Proportional_Allocation.xlsx")
print(f"  4. {dim_example}_Three_Approaches_Comparison.csv")

print("\n" + "="*60)
print("ANALYSIS COMPLETE!")
print("="*60)


# In[ ]:




