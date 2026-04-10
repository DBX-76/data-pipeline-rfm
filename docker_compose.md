``` mermaid
graph TD
    subgraph subHost["Docker Host"]
        
        subgraph "Réseau: rfm-network"
            
            subgraph subAirflow["Écosystème Airflow"]
                A[Airflow Webserver<br/>Port 8080]
                B[Airflow Scheduler]
                C[(Postgres Airflow<br/>DB: airflow)]
                
                A -->|Lit/Écrit l'état des DAGs| C
                B -->|Lit/Écrit l'état des DAGs| C
                A -.->|Orchestre| B
            end
            style subAirflow fill:#85c1e9,stroke:#2e86c1,stroke-width:2px,color:#000

            subgraph subRFM["Écosystème Projet RFM"]
                D[(Postgres Business<br/>DB: rfm_db<br/>Port 5432)]
                E[📂 Dossier /scripts<br/>Monté en volume]
                H[Metabase<br/>Port 3000]
                
                B -->|Exécute les scripts Python| E
                E -->|Lit/Écrit données RFM| D
                H -->|Visualise les données| D
            end
            style subRFM fill:#a3d5df,stroke:#5dade2,stroke-width:2px,color:#000
            
            %% Liaison critique : Le scheduler lit les DAGs et les scripts
            B -.->|Monte le volume ./scripts| E
        end

        %% Volumes persistants
        F[(Volume: postgres-airflow-data)]
        G[(Volume: postgres-business-data)]
        
        C --- F
        D --- G
    end
    style subHost fill:#d6eaf8,stroke:#5dade2,stroke-width:2px,color:#000

    %% Utilisateurs externes
    User((👤 Utilisateur)) -->|Interface Web : localhost:8080| A
    User -->|Dashboards : localhost:3000| H
    Dev((👨‍💻 Développeur)) -->|Connexion DB : localhost:5432| D
    Dev -->|Édition Code| E

    style A fill:#2c3e50,stroke:#3498db,stroke-width:2px,color:#fff
    style B fill:#2c3e50,stroke:#3498db,stroke-width:2px,color:#fff
    style C fill:#e74c3c,stroke:#c0392b,stroke-width:2px,color:#fff
    style D fill:#27ae60,stroke:#2ecc71,stroke-width:2px,color:#fff
    style E fill:#f39c12,stroke:#d35400,stroke-width:2px,color:#fff
    style H fill:#3498db,stroke:#2980b9,stroke-width:2px,color:#fff
    style User fill:#95a5a6,stroke:#7f8c8d,color:#fff
    style Dev fill:#95a5a6,stroke:#7f8c8d,color:#fff
```
    