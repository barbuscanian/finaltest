-- 1 --
-- Please create dimension tables dim_user , dim_post , and dim_date to store normalized data from the raw tables --
-- Create dimension tables
CREATE TABLE dim_user (
    user_id INT PRIMARY KEY,
    user_name VARCHAR(100),
    country VARCHAR(50)
);

CREATE TABLE dim_post (
    post_id INT PRIMARY KEY,
    post_text VARCHAR(500),
    user_id INT,
    FOREIGN KEY (user_id) REFERENCES dim_user(user_id)
);

CREATE TABLE dim_date (
    date_id INT PRIMARY KEY,
    full_date DATE,
    year INT,
    month INT,
    day INT
);

-- 2 --
-- Populate the dimension tables by inserting data from the related raw tables
-- Insert data into dimension tables
INSERT INTO dim_user
SELECT DISTINCT user_id, user_name, country FROM raw_users;

INSERT INTO dim_post
SELECT DISTINCT post_id, post_text, user_id FROM raw_posts;

INSERT INTO dim_date
SELECT DISTINCT
    ROW_NUMBER() OVER () AS date_id,
    post_date::date AS full_date,
    EXTRACT(YEAR FROM post_date) AS year,
    EXTRACT(MONTH FROM post_date) AS month,
    EXTRACT(DAY FROM post_date) AS day
FROM raw_posts;

-- 3 --
-- Create a fact table called fact_post_performance to store metrics like post views and likes over time -- 
-- Create fact table
CREATE TABLE fact_post_performance (
    post_id INT,
    date_id INT,
    post_views INT,
    like_count INT,
    PRIMARY KEY (post_id, date_id),
    FOREIGN KEY (post_id) REFERENCES dim_post(post_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);

-- 4 --
-- Populate the fact table by joining and aggregating data from the raw tables --
-- Insert data into fact table
INSERT INTO fact_post_performance (post_id, date_id, post_views, like_count)
SELECT
    rp.post_id,
    dd.date_id,
    COUNT(DISTINCT rl.user_id) AS post_views,
    COUNT(DISTINCT rl.like_id) AS like_count
FROM raw_posts rp
JOIN dim_date dd ON rp.post_date = dd.full_date
LEFT JOIN raw_likes rl ON rp.post_id = rl.post_id
GROUP BY rp.post_id, dd.date_id;

-- 5 --
-- Please create a fact_daily_posts table to capture the number of posts per user per day --
-- Create fact daily posts table
CREATE TABLE fact_daily_posts (
    user_id INT,
    date_id INT,
    posts_count INT,
    PRIMARY KEY (user_id, date_id),
    FOREIGN KEY (user_id) REFERENCES dim_user(user_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);


-- 6 --
-- Also populate the fact table by joining and aggregating data from the raw tables --
-- Insert data into fact table
INSERT INTO fact_daily_posts (user_id, date_id, posts_count)
SELECT
    rp.user_id,
    dd.date_id,
    COUNT(rp.post_id) AS posts_count
FROM raw_posts rp
JOIN dim_date dd ON rp.post_date = dd.full_date
GROUP BY rp.user_id, dd.date_id;
