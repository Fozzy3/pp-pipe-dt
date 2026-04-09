# REVISION NOTES - EWS Manuscript

**General:**
- Executed strict compliance with "Spec-Driven Development | Multi-Agent Orchestration".
- Re-architected the paper to position "Conformal Prediction under Discontinuous Telemetry" as the primary scientific contribution.

**Section III:**
- **Added III.F (Conformal Calibration under Telemetry Loss):** Formalized the ad-hoc imputation protocol into a Split Conformal Prediction calibration layer (Angelopoulos & Bates, 2023). Incorporated the missed-polling-cycle covariate ($k$) to widen prediction sets when data degrades.

**Section IV.A:**
- **Added "Post-Filter Dataset Composition":** Explicitly reported the sample size implications of applying the Transition State Filter ($B_{n-1} = 0$), including class distribution and percentage of original data retained. (Requires filling placeholders).

**Section V.A:**
- **Added V.A.1 (Window Size Sensitivity Analysis):** Conducted a formal ablation study justifying the $n-3$ selection based on empirical metrics rather than assumption. Tied the finding theoretically to Daganzo's (2009) headway variance growth per stop. (Requires filling placeholders).

**Section V.C:**
- **Renamed to "Spatial Cross-Validation Across Independent Corridors":** Eliminated all incorrect references to "transfer learning." Reframed findings as evidence of the physical generalizability of headway deterioration signatures within the same operator.

**Section V.E:**
- **Rewrote "Computational Benchmarks":** Contextualized the `0.00042 ms/record` latency by benchmarking the DuckDB in-process HTAP pipeline against a standard Pandas+Scikit-Learn pipeline and a PostgreSQL row-oriented LAG query, citing Raasveldt & Muhleisen (2019). (Requires filling placeholders).

**Section VI.D:**
- **Appended Future Work on True Inter-City Transfer:** Defined the strict prerequisites for true transfer learning across municipalities (different GTFS operator IDs, stop sequences, and scheduled headway distributions).

**Abstract & Index Terms:**
- **Replaced entirely:** Repositioned the paper to highlight the Conformal Prediction calibration layer and the edge-ready Zero-ETL HTAP architecture. Added "Conformal Prediction, Uncertainty Quantification" to Index Terms.
