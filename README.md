# MigratingTable

MigratingTable is a wrapper library for [Azure Table storage](https://msdn.microsoft.com/en-us/library/azure/dd179423.aspx) that allows data to be migrated from one underlying table to another transparently to the application.  See [these slides](doc/intern-final-presentation.pptx) for information about the motivation and design.

The initial focus in developing MigratingTable was to put it through systematic concurrency testing using [P#](http://p-org.github.io/PSharp) to gain confidence in its correctness.  Consequently, this open source release includes the P# test harness, but not some of the adapters necessary to actually use MigratingTable on real Azure tables.  MigratingTable also accepts an optional parameter to enable various "bugs" to demonstrate the ability of the test to find them.  As development continues, it may make sense to remove the "bug" code paths to make the code easier to read.

MigratingTable was initially developed by Matt McCutchen (personal email: matt@mattmccutchen.net) while an intern at Microsoft in summer 2015 with manager John Erickson <John.Erickson@microsoft.com>.  We decided to make it available to the public even though we are out of time to prepare complete public documentation.  If you're interested, by all means please contact us so we can prioritize improving the documentation or reviewing existing Microsoft internal documentation for public release.

## Building and running the P# test

This procedure assumes you have Visual Studio 2015 installed on Windows.

1. Run ```git submodule update --init``` to check out the fork of P# required by the test (if not already done).
2. Open and build ```PSharp\PSharp.sln``` with Visual Studio.
3. Open and build ```Migration.sln``` with Visual Studio.  This should automatically download the required NuGet packages.
4. Run:
```
PSharp\Binaries\Debug\PSharpCompiler.exe /s:Migration.sln /test:Migration
```
Add any desired P# flags, e.g., ```/i:1000``` for number of iterations or ```/schseed:314159``` to set the starting random seed for hopefully reproducible behavior.  To enable one of the "bugs", add ```/testvar:mtablebug=N``` where N is the integer value of the MTableOptionalBug enum constant.

## Using in production (if you dare)

Starting with a live application using the "old" table, to migrate to the "new" table:

1. Write an adapter that wraps a CloudTable and implements IChainTable2 and an implementation of IConfigurationService<MTableConfiguration>.  (Microsoft has both but they could use polishing.)
2. Push MasterMigratingTable.INITIAL_CONFIGURATION to the configuration service.
3. Update your application processes to use a MigratingTable instance backed by the same old and new tables and configuration service.  This can be done one at a time; MigratingTable in its initial state is compatible with direct access to the old table.
4. Have a "migrator" process construct a MasterMigratingTable and call MigrateAsync.
5. Update your application processes to access the new table directly.  This can also be done one at a time.

## Known bugs in MigratingTable

- The DeleteIfExists implementation for USE_NEW_HIDE_METADATA state, that just omits the operation from the batch, is not linearizable.  It can also fail by generating a zero-length batch.  The second part is easy to fix, but we should probably remove DeleteIfExists from IChainTable2 unless/until we can fix the first part.

## Known bugs in the test harness

- A missing annotation in the MigratingTable code (an easy mistake to make, which I actually made once) will cause all machines to become blocked, and P# considers that a success.  We need to enable liveness checking to catch this.
