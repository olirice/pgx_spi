use pgx::*;
use std::panic;

pg_module_magic!();

extension_sql!(
    r#"   
create table example (
    id serial8 not null primary key,
    title text
);

insert into example(title) values ('a'), ('b');
"#,
    name = "savepoint_example",
);

#[pg_extern]
fn spi_rollback_test() -> Vec<String> {
    let x: Vec<String> = vec![];

    Spi::connect_ext(true, |mut client| {
        let res = client
            .update(
                "insert into example(title) values ('a') returning title;",
                None,
                None,
            )
            .first()
            .get_one::<String>()
            .expect("errrr");

        client.rollback();

        Ok(Some(vec![res]))
    });

    x
}

// Goal: Function exits without an error and no record is inserted into `example` table.

use pgx::*;

trait SpiNonAtomic {
    /// execute SPI commands via the provided `SpiClient` and return a value from SPI which is
    /// automatically copied into the `CurrentMemoryContext` at the time of this function call
    ///
    /// Setting `nonatomic` to `true` allows transaction control calls (SPI_commit, SPI_rollback).
    /// Otherwise, calling those functions will result in an immediate error.
    fn connect_ext<
        R: FromDatum + IntoDatum,
        F: FnOnce(SpiClient) -> std::result::Result<Option<R>, SpiError>,
    >(
        nonatomic: bool,
        f: F,
    ) -> Option<R> {
        let outer_memory_context =
            PgMemoryContexts::For(PgMemoryContexts::CurrentMemoryContext.value());

        /// a struct to manage our SPI connection lifetime
        struct SpiConnection;
        impl SpiConnection {
            /// Connect to Postgres' SPI system
            fn connect_ext(flags: u32) -> Self {
                // connect to SPI
                Spi::check_status(unsafe { pg_sys::SPI_connect_ext(flags as _) });
                SpiConnection
            }
        }

        impl Drop for SpiConnection {
            /// when SpiConnection is dropped, we make sure to disconnect from SPI
            fn drop(&mut self) {
                // disconnect from SPI
                Spi::check_status(unsafe { pg_sys::SPI_finish() });
            }
        }

        // connect to SPI
        let flags = if nonatomic {
            pg_sys::SPI_OPT_NONATOMIC
        } else {
            0
        };
        let _connection = SpiConnection::connect_ext(flags);

        // run the provided closure within the memory context that SPI_connect()
        // just put us un.  We'll disconnect from SPI when the closure is finished.
        // If there's a panic or elog(ERROR), we don't care about also disconnecting from
        // SPI b/c Postgres will do that for us automatically
        match f(SpiClient) {
            // copy the result to the outer memory context we saved above
            Ok(result) => {
                // we need to copy the resulting Datum into the outer memory context
                // *before* we disconnect from SPI, otherwise we're copying free'd memory
                // see https://github.com/zombodb/pgx/issues/17
                let copied_datum = match result {
                    Some(result) => {
                        let as_datum = result.into_datum();
                        if as_datum.is_none() {
                            // SPI function returned Some(()), which means we just want to return None
                            None
                        } else {
                            unsafe {
                                R::from_datum_in_memory_context(
                                    outer_memory_context,
                                    as_datum.expect("SPI result datum was NULL"),
                                    false,
                                    pg_sys::InvalidOid,
                                )
                            }
                        }
                    }
                    None => None,
                };

                copied_datum
            }

            // closure returned an error
            Err(e) => panic!("{:?}", e),
        }
    }
}

impl SpiNonAtomic for Spi {}

trait SpiRollback {
    /// Commits the current transaction. It is approximately equivalent to running the SQL command COMMIT.
    /// After the transaction is committed, a new transaction is automatically started using default transaction
    /// characteristics, so that the caller can continue using SPI facilities. If there is a failure during commit,
    /// the current transaction is instead rolled back and a new transaction is started, after which the error is
    /// thrown in the usual way.
    ///
    /// Can only be executed if the SPI connection has been set as `nonatomic` in the call to `Spi::connect`.
    fn commit(&self) {
        unsafe { pg_sys::SPI_commit() }
    }

    /// Commits the current transaction same as `commit`, but the new transaction is started with the same transaction
    /// characteristics as the just finished one, like with the SQL command COMMIT AND CHAIN.
    ///
    /// Can only be executed if the SPI connection has been set as `nonatomic` in the call to `Spi::connect`.
    fn commit_and_chain(&self) {
        unsafe { pg_sys::SPI_commit_and_chain() }
    }

    /// Rolls back the current transaction. It is approximately equivalent to running the SQL command ROLLBACK.
    /// After the transaction is rolled back, a new transaction is automatically started using default transaction
    /// characteristics, so that the caller can continue using SPI facilities.
    ///
    /// Can only be executed if the SPI connection has been set as `nonatomic` in the call to `Spi::connect`.
    fn rollback(&self) {
        unsafe { pg_sys::SPI_rollback() }
    }

    /// Rolls back the current transaction same as `rollback`, but the new transaction is started with the same
    /// transaction characteristics as the just finished one, like with the SQL command ROLLBACK AND CHAIN.
    fn rollback_and_chain(&self) {
        unsafe { pg_sys::SPI_rollback_and_chain() }
    }
}

impl SpiRollback for SpiClient {}
