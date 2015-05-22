module.exports = (next) ->*
  @db = yield @pg.connect-pool "postgres://postgres@#{olio.config.pg.host or 'localhost'}/#{olio.config.pg.db}"
  this <<< @db.model
  for key of @db.model
    try
      this[key] <<< require "#{process.cwd!}/lib/#key"
  this <<< @db{exec, first, relate, estrange, related, relation, save, destroy, wrap}
  yield @db.exec 'BEGIN'
  try
    yield next
    yield @db.exec 'COMMIT'
  catch e
    yield @db.exec 'ROLLBACK'
    throw e
  finally
    @db.release!
