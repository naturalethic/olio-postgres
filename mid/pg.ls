module.exports = (next) ->*
  @pg = yield olio.pg.connect-pool "postgres://postgres@#{olio.config.pg.host or 'localhost'}/#{olio.config.pg.db}"
  this <<< @pg.model
  this <<< @pg{exec, first, relate, estrange, related, relation, save, destroy, wrap}
  yield @pg.exec 'BEGIN'
  try
    yield next
    yield @pg.exec 'COMMIT'
  catch e
    yield @pg.exec 'ROLLBACK'
    throw e
  finally
    @pg.release!
