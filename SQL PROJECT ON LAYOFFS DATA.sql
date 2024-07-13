-- Data Cleaning

-- SELECTING THE REQUIRED TABLE FROM THE DATABASE
USE world_layoffs;
SELECT * FROM layoffs;

-- STEPS INVOLVED
-- 1. REMOVE DUPLICATES IF THERE ARE ANY
-- 2. STANDARDIZE THE DATA
-- 3. CHECKING FOR NULL VALUES
-- 4. REMOVE UNNECESSARY ROWS AND COLUMNS IF ANY

-- CREATING A NEW TABLE WITH DATA IN LAYOFFS
CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- 1. IDENTIFYING AND REMOVING DUPLICATES
SELECT *
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE COMPANY ='Casper';

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
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num >1;

DELETE
FROM layoffs_staging2
WHERE row_num >1;


SELECT *
FROM layoffs_staging2;

-- 2. STANDARDIZING THE DATA

--  REMOVING WHITE SPACE FROM COMPANY NAME
SELECT company,(TRIM(company))
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- STANDARDIZING CRYPTO, CRYPTO CURRENCY, CRYPTOCURRENCY
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry ='Crypto'
WHERE industry LIKE 'Crypto%';

-- STANDARDIZING COUNTRY NAME

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_staging2
WHERE country LIKE 'United States%'
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- CHANGING THE DTYPE OF DATE

SELECT `date`,
str_to_date(`date`,'%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`,'%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3. DEALING WITH NULL VALUES

SELECT DISTINCT industry
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

UPDATE layoffs_staging2
SET industry = null
WHERE industry = '';

SELECT t1.industry,t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry ='')
AND t2.industry IS NOT NULL;


UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2
WHERE company = 'Bally''s Interactive';

SELECT *
FROM layoffs_staging2;

-- 4. REMOVE UNNECESSARY ROWS AND COLUMNS IF ANY

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


-- Exploratory Data Analysis--

USE world_layoffs;

SELECT *
FROM layoffs_staging2;

-- CHECKING THE MAXIMUM VALUES IN TOTAL LAID OFF AND PERCENTAGE LAID OFF --
SELECT MAX(total_laid_off),MAX(percentage_laid_off)
FROM layoffs_staging2;

-- DISPLAYING DATA OF WHOSE PERECNTAGE LAID OFF IS 1 AND ORDER THE DATA BY TOTAL LAID OFF--
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off=1
ORDER BY total_laid_off DESC;

-- DISPLAYING DATA OF WHOSE PERECNTAGE LAID OFF IS 1 AND ORDER THE DATA BY FUNDS RAISED IN MILLIONS--
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off=1
ORDER BY funds_raised_millions DESC;

-- DISPLAYING COMPANIES TOTAL LAID OFF IN DESCENDING ORDER--
SELECT company,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- DISPLAYING THE TIME FRAME OF DATASET--
SELECT MIN(date),MAX(date)
FROM layoffs_staging2;

-- DISPLAYING INDUSTRIES TOTAL LAID OFF IN DESCENDING ORDER--
SELECT industry,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- DISPLAYING COUNTRIES TOTAL LAID OFF IN DESCENDING ORDER--
SELECT country,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- DISPLAYING TOTAL LAID OFF IN EACH YEAR IN DESCENDING ORDER--
SELECT YEAR(`date`),SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

-- DISPLAYING STAGE TOTAL LAID OFF IN DESCENDING ORDER--
SELECT stage,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

-- PROGRESSION OF LAYOFFS--

-- Getting month from date--
SELECT SUBSTRING(`date`,6,2) AS `MONTH`
FROM layoffs_staging2;

-- Displaying layoffs in each month--
SELECT SUBSTRING(`date`,1,7) AS `MONTH`,SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

-- Getting monthwise rolling total of layoffs--
WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`,1,7) AS `MONTH`,SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`,total_off
,SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;

-- Displaying yearly total laid off by companies--

SELECT company,YEAR(`date`),SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company,YEAR(`date`)
ORDER BY company ASC;

-- Displaying yearly total laid off by companies in descending order--
SELECT company,YEAR(`date`),SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company,YEAR(`date`)
ORDER BY 3 DESC;

-- Ranking companies based on yearly total laid off --
-- Dispalying the top 5 companies in yearly total laid off--

WITH Company_Year(COMPANY,YEARS,TOTAL_LAID_OFF) AS
(
SELECT company,YEAR(`date`),SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company,YEAR(`date`)
), Company_Year_Rank AS 
(SELECT *, DENSE_RANK() OVER(PARTITION BY YEARS ORDER BY TOTAL_LAID_OFF DESC) AS RANKING
FROM Company_Year
WHERE YEARS IS NOT NULL
)
SELECT * 
FROM  Company_Year_Rank
WHERE RANKING <=5
;

-- Ranking Industries based on yearly total laid off --
-- Dispalying the top 5 Industries in yearly total laid off--

WITH Industry_Year(INDUSTRY,YEARS,TOTAL_LAID_OFF) AS
(
SELECT industry,YEAR(`date`),SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry,YEAR(`date`)
), Industry_Year_Rank AS 
(SELECT *, DENSE_RANK() OVER(PARTITION BY YEARS ORDER BY TOTAL_LAID_OFF DESC) AS RANKING
FROM Industry_Year
WHERE YEARS IS NOT NULL
)
SELECT * 
FROM  Industry_Year_Rank
WHERE RANKING <=5
;

-- Ranking Countries based on yearly total laid off --
-- Dispalying the top 5 Countries in yearly total laid off--

WITH Country_Year(COUNTRY,YEARS,TOTAL_LAID_OFF) AS
(
SELECT country,YEAR(`date`),SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country,YEAR(`date`)
), Country_Year_Rank AS 
(SELECT *, DENSE_RANK() OVER(PARTITION BY YEARS ORDER BY TOTAL_LAID_OFF DESC) AS RANKING
FROM Country_Year
WHERE YEARS IS NOT NULL
)
SELECT * 
FROM  Country_Year_Rank
WHERE RANKING <=5
;

-- Ranking Stage based on yearly total laid off --
-- Dispalying the top 3 Stages in yearly total laid off--

WITH Stage_Year(STAGE,YEARS,TOTAL_LAID_OFF) AS
(
SELECT stage,YEAR(`date`),SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage,YEAR(`date`)
), Stage_Year_Rank AS 
(SELECT *, DENSE_RANK() OVER(PARTITION BY YEARS ORDER BY TOTAL_LAID_OFF DESC) AS RANKING
FROM Stage_Year
WHERE YEARS IS NOT NULL
)
SELECT * 
FROM  Stage_Year_Rank
WHERE RANKING <=3
;
