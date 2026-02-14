**Project Overview**

This project implements a comprehensive Anti-Money Laundering (AML) Transaction Monitoring System using Microsoft Fabric's medallion architecture. The system processes and analyzes synthetic transaction data to
identify suspicious patterns and potential money laundering activities, providing compliance officers and financial investigators with actionable insights through an interactive Power BI dashboard.

**Architecture**

***Medallion Architecture Implementation***

<img width="1035" height="479" alt="Screenshot (624)" src="https://github.com/user-attachments/assets/c4c2c61f-de68-4ac6-add8-650c3ddd3d56" />

**Business Objectives**

-Detect suspicious transaction patterns in real-time

-Identify high-risk transaction corridors and typologies

-Provide compliance teams with intuitive visualization tools

-Enable data-driven investigation prioritization

**Dataset**
Source
The project uses the SAML-D (Synthetic Anti-Money Laundering Dataset) from Kaggle


**Technical Stack**

***Microsoft Fabric Components:***

-Lakehouse: Central data repository with Delta format

-Notebooks: PySpark data transformations

<img width="1920" height="908" alt="Screenshot (617)" src="https://github.com/user-attachments/assets/0a1cd28e-c7c9-4ab2-ae1c-b2b18d6ec84b" />    <img width="1920" height="911" alt="Screenshot (618)" src="https://github.com/user-attachments/assets/d2136d7b-5ac1-443f-adee-689086e2e73e" />   <img width="1920" height="913" alt="Screenshot (621)" src="https://github.com/user-attachments/assets/04f84b43-a9da-4700-8ff3-ea32675e55bd" />




-Data Factory: Pipeline orchestration

-Power BI: Interactive dashboard

<img width="1255" height="736" alt="Screenshot (623)" src="https://github.com/user-attachments/assets/c6147f87-b1b5-4913-927b-62637fb7e310" />


-SQL Analytics Endpoint: Semantic model management

