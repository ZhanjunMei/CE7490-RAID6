# CE7490-RAID6

## Overview
This project presents the design and implementation of a RAID6-based storage system developed using Python. RAID6 is a robust storage solution offering both redundancy and data protection against up to two simultaneous disk failures, making it ideal for large-scale storage environments. The system efficiently handles data storage, retrieval, and recovery, ensuring data integrity even in the event of disk failures.

## Key Features
1. **Accurate Data Storage and Retrieval**  
   Ensures reliable access to information with precise storage and retrieval mechanisms.
   
2. **Lost Block Detection**  
   Incorporates a mechanism to detect lost or corrupted storage blocks, safeguarding data integrity.

3. **Block Reconstruction with Galois Field (GF) Operations**  
   Uses Galois field arithmetic to reconstruct lost or damaged blocks, providing resilience against up to two simultaneous disk failures.

4. **Variable-Sized File Storage**  
   Supports flexible file management by enabling storage of variable-sized files.

5. **Consistency During Modifications**  
   Guarantees file consistency and integrity during data modifications, preventing corruption or data loss.

6. **Scalability to Large Configurations**  
   Supports up to 255+2 disk configurations, allowing the system to scale to meet the demands of extensive storage environments.

7. **Computational Efficiency**  
   Optimized with a table-based mechanism to record intermediate results for Galois field calculations, improving computational efficiency during data recovery and storage operations.




