compose-environment = (pg) ->
  env = {}
  if pg
    env <<< pg{exec, first, relate, related, relation, save, wrap, estrange} <<< pg.model
  for name, lib of olio.lib
    if env[name]
      env[name] <<< lib
    else
      if typeof! lib == \Function
        env[name] = lib
      else
        env[name] = {} <<< lib
      if pg
        env[name] <<< pg{exec, first, relate, estrange, related, relation, save, destroy, wrap}
  all-names = (pg and unique((keys olio.lib) ++ (keys pg.model))) or keys olio.lib
  for n1 in all-names
    for n2 in all-names
      continue if n1 == n2
      env[n1][n2] = env[n2]
  env

module.exports = (next) ->*
  @db = yield olio.lib.pg.connect-pool "postgres://postgres@#{olio.config.pg.host or 'localhost'}/#{olio.config.pg.db}"
  @ <<< compose-environment @db
  yield @db.exec 'BEGIN'
  try
    yield next
    yield @db.exec 'COMMIT'
  catch e
    yield @db.exec 'ROLLBACK'
    throw e
  finally
    @db.release!
