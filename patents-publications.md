### Cuantas patentes hay disponibles bajo el CPC code A01N dentro de los países de SudAmerica, NA, Alemania y Japón?

```sql
SELECT 
    publication_number,
    country_code,
    title_localized,
    abstract_localized,
    claims_localized,
    description_localized,
    publication_date,
    filing_date,
    grant_date,
    inventor_harmonized,
    assignee_harmonized,
    cpc.code,
    cpc.inventive,
    cpc.first,
    cpc.tree,
    citation,
    parent,
    child
FROM patents-public-data.patents.publications,
UNNEST(cpc) as cpc
WHERE country_code IN ('US', 'JP', 'DE', 'AR', 'BR', 'CL', 'EC', 'PY', 'PE', 'VE', 'BO')
AND cpc.code LIKE 'A01N%'
```

621,029

### Cuantas de estas patentes están en idioma ingles, y cuantas están en otros idiomas, por país.

```sql
SELECT
    country_code,
    abstract.language, 
    COUNT(*) q
FROM `acn-researchplatform.ad_hocs.publications_agrochemicals`,
UNNEST(abstract_localized) as abstract
GROUP BY abstract.language, country_code
ORDER BY country_code ASC, q DESC
```

| Country_code | Language | Patents|
|--------------|----------|--------|
| AR           | es       |  26459 |
| AR           | en       |   8040 |
| BR           | pt       |  55896 |
| BR           | en       |  47188 |
| CL           | es       |   8513 |
| CL           | en       |   4415 |
| DE           | en       |  19087 |
| DE           | de       |  10525 |
| EC           | es       |   3765 |
| EC           | en       |   1500 |
| JP           | en       | 114188 |
| JP           | ja       |  91726 |
| PE           | es       |   4379 |
| PE           | en       |   1695 |
| US           | en       | 379143 |

### Cuantas de estas patentes están registradas en más de un país

```sql
SELECT title.text, COUNT(DISTINCT country_code) q
FROM `acn-researchplatform.ad_hocs.publications_agrochemicals`,
UNNEST(title_localized) as title
GROUP BY title.text
HAVING q > 1;
```

Solo 3472 patentes están registradas en más de un país

### Cuantas de estas patentes son apendices a invenciones ya registradas.

```sql

```


### Ejemplo de patente
```sql
SELECT *
FROM `acn-researchplatform.ad_hocs.publications_agrochemicals`,
UNNEST(title_localized) as title
WHERE title.text = 'COMBINATIONS OF NEMATICIDE, INSECTICIDE AND ACARICIDAL ACTIVE PRINCIPLES INCLUDING PYRIDYLETHYLBENZAMIDE AND INSECTICIDES'
```