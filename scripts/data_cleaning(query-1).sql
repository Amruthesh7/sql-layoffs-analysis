-- DATA CLEANING

SELECT *
FROM layoffs
;

-- STEPS
-- 1.0 remove Duplicates
-- 2.0 Standarise the Data
-- 3.0  Null Values or Blank Values
-- 4.0 Remove any columns Or rows


CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging;

SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging;

WITH CTE_duplicate AS
(
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM CTE_duplicate
WHERE ROW_NUM > 1
;

SELECT *
FROM layoffs_staging
WHERE company='Casper'
;


WITH CTE_duplicate AS
(
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging
)
DELETE 
FROM CTE_duplicate
WHERE ROW_NUM > 1
;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging;

DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2
;

-- Standardising data

SELECT company,TRIM(company)
FROM layoffs_staging2
;

UPDATE layoffs_staging2
SET company = TRIM(company)
;

SELECT DISTINCT(industry)
FROM layoffs_staging2
ORDER BY 1
;
SELECT company,industry
FROM layoffs_staging2
WHERE country='India' 
; 

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%'
;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'
;

SELECT industry
FROM layoffs_staging2
;

SELECT DISTINCT(location) 
FROM layoffs_staging2
ORDER BY 1
;

SELECT DISTINCT(country) 
FROM layoffs_staging2
ORDER BY 1
;

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States%'
ORDER BY 1
;

SELECT DISTINCT country,TRIM(TRAILING '.'  FROM country) AS NEW_ONE
FROM layoffs_staging2
ORDER BY 1
;

SELECT `date`,
STR_TO_DATE(`date`,'%m/%d/%Y')
FROM layoffs_staging2
;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%'
;

SELECT date,
STR_TO_DATE(`date`,'%m/%d/%Y')
FROM layoffs_staging2
;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y')
;

SELECT `date`
FROM layoffs_staging2
;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` date
;


SELECT *
FROM layoffs_staging2
;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

SELECT DISTINCT industry
FROM layoffs_staging2
;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry=''
;

SELECT *
FROM layoffs_staging2
WHERE company='Airbnb'
;

SELECT t1.industry,t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
 ON t1.company = t2.company
 AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry='')
AND t2.INDUSTRY IS NOT NULL
;


UPDATE layoffs_staging2
SET industry = NULL
WHERE industry=''
;


UPDATE layoffs_staging2 t1
JOIN layoffs_staging2  t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry ='')
AND t2.industry IS NOT NULL
;


SELECT t1.industry,t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
 ON t1.company = t2.company
 AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry='')
AND t2.INDUSTRY IS NOT NULL
;

SELECT *
FROM layoffs_staging2
WHERE company='Airbnb'
;

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%'
;

SELECT *
FROM layoffs_staging2
;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

SELECT *
FROM layoffs_staging2
;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num
;