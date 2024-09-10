WITH CTE AS (  
    SELECT   
        c1.concept_name AS reg_name,  
        string_agg(lower(c2.concept_name), ',' ORDER BY lower(c2.concept_name) ASC) AS combo_name,  
        c1.concept_id  
    FROM   
        @cdmDatabaseSchema.concept_relationship  
    JOIN   
        @cdmDatabaseSchema.concept c1   
        ON c1.concept_id = concept_id_1  
    JOIN   
        @cdmDatabaseSchema.concept c2   
        ON c2.concept_id = concept_id_2  
    WHERE   
        c1.vocabulary_id = 'HemOnc'   
        AND relationship_id = 'Has antineoplastic'  
    GROUP BY   
        c1.concept_name, c1.concept_id  
    ORDER BY   
        c1.concept_name  
),
WITH CTE_second AS (  
    SELECT   
        c.*,   
        CASE   
            WHEN lower(c.reg_name) = regexp_replace(c.combo_name, ',', ' and ', 'g') THEN 0  
            ELSE row_number() OVER (PARTITION BY c.combo_name ORDER BY length(c.reg_name))   
        END AS rank  
    FROM   
        CTE c  
    ORDER BY   
        rank DESC  
),
CTE_third as (
select *,min(rank) over (partition by combo_name)
from CTE_second 
),
CTE_fourth as (
select ct.reg_name, ct.combo_name, ct.concept_id 
from CTE_third ct
where rank = min
)
select * 
into @writeDatabaseSchema.@vocabularyTable
from CTE_fourth
