use sqlx::postgres::PgPoolOptions;

pub async fn write_to_pg(database_url: &str) -> Result<(), sqlx::Error> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await?;
    println!("Connected to PostgreSQL!");

    // Create table
    sqlx::query("CREATE TABLE IF NOT EXISTS test (id INT, name VARCHAR(255));")
        .execute(&pool)
        .await?;

    // Insert data
    sqlx::query(
        "INSERT INTO test VALUES (1, 'postgres'), (2, 'pg-admin'), (3, 'docker')
         ON CONFLICT DO NOTHING;",
    )
    .execute(&pool)
    .await?;

    // Query and print results
    let rows = sqlx::query("SELECT id, name FROM test")
        .fetch_all(&pool)
        .await?;

    println!("\nResults from database:");
    for row in rows {
        println!("{row:?}");
    }

    Ok(())
}
