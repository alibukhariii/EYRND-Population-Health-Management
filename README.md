Date: August 1st, 2025

Programmer: Ali Bukhari, System Planner & Data Analyst, Eastern York Region & North Durham (EYRND) Ontario Health Team (OHT)

Description:
This repository contains files related to the Population Health Management Plan for the EYRND catchment area.

Projects:

1. Base files: Contains DA and FSA crosswalks sourced from Statistics Canada 2021 Census boundary files --> output = a DA x FSA crosswalk with DA-level proportion weights for each FSA
              The main manipulations involved here using Python were:
               1. Obtaining DA identifiers from Statistics Canada --> Subset to Ontario
               2. Mapping each DA to a singular FSA via the PCCF+ 2021 crosswalk. In cases where DAs straddle more than one FSA, the proportion of postal codes within that DA that belong to an FSA were calculated and all future measures (e.g., FSA-level population) were weighed based on this proportion.
3. Population data from Statistics Canada: Pulled from CanCensus API in R for all DAs in the EYRND catchment area
   - Population of each 5-year age group by sex
   - etc
5. d
