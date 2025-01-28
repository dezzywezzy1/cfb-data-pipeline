WITH cleaned_calendar AS (
    SELECT
        lastGameStart as last_game_start,
        firstGameStart as first_game_start,
        seasonType as season_type, 
        week, 
        season as year
    FROM `cfb-pipeline-project.calendar.staging_table`
)

SELECT
    last_game_start,
    first_game_start,
    season_type,
    week, 
    year
FROM cleaned_calendar