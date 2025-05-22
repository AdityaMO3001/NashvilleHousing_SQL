-- My SQL Data Cleaning Project
-- Looking at layoff data from 2022

SELECT * 
FROM world_layoffs.layoffs;

-- Making a copy of the data to work with
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs;

-- Steps we'll take:
-- 1. Find and remove duplicates
-- 2. Clean up the data
-- 3. Check for nulls
-- 4. Remove stuff we don't need

-- 1. Finding duplicates

SELECT *
FROM world_layoffs.layoffs_staging;

-- Looking for duplicates in main fields
SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`) AS row_num
	FROM 
		world_layoffs.layoffs_staging;

-- Checking for exact duplicates
SELECT *
FROM (
	SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;
    
-- Double checking a specific company
SELECT *
FROM world_layoffs.layoffs_staging
WHERE company = 'Oda'
;

-- Looking at all possible duplicates
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;

-- Making a new table to handle duplicates
CREATE TABLE `world_layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);

-- Adding row numbers to find duplicates
INSERT INTO `world_layoffs`.`layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging;

-- Getting rid of duplicates
DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num >= 2;

-- 2. Cleaning up the data

SELECT * 
FROM world_layoffs.layoffs_staging2;

-- Checking what industries we have
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

-- Finding empty industry fields
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- Making empty fields NULL
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Filling in missing industry data
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Making all crypto entries the same
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- Fixing country names
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- Fixing date format
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3. Checking nulls

-- Looking at missing layoff numbers
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;

-- Finding rows with missing data
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Removing rows we can't use
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Cleaning up: removing the row_num column
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Final look at our cleaned data
SELECT * 
FROM world_layoffs.layoffs_staging2;


































