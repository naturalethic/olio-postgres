require! \pg
require! \knex
require! \moment

knex = knex client: 'pg'

promisify-all pg
promisify-all pg.Client.prototype

pg.types.set-type-parser 1182, ->
  it and moment(it)

pg.types.set-type-parser 1184, ->
  it and moment(it)

pg.types.set-type-parser 1186, ->
  it and moment.duration(it)

pg.types.set-type-parser 1700, ->
  it and parse-float it

exec = (connection, statement, ...args) ->*
  statement = statement.to-string! if typeof! statement != 'String'
  args = args[0] if args.length == 1 and typeof! args[0] == 'Array'
  args = args |> map ->
    if it and it.to-ISO-string
      return it.to-ISO-string!
    it
  exec.i = 0
  statement = statement.replace /\?\?/g, 'JSONB_QUESTION'
  statement = statement.replace /\?/g, -> exec.i += 1; '$' + exec.i
  statement = statement.replace /JSONB_QUESTION/g, '?'
  statement = statement.replace /\w+\-\w+/g, -> if camelized[it] then "\"#{camelized[it]}\"" else it
  return (yield connection.query-async statement, args).rows

exec-first = (connection, statement, ...args) ->*
  it = yield exec connection, statement + ' LIMIT 1', ...args
  return it.length and it[0] or null

save = (connection, source, properties = {}) ->*
  for key in keys properties
    if key in columns[source._table]
      source._record[key] = delete properties[key]
  if source._record.properties
    for key in keys properties
      properties[key] = undefined if properties[key] == false
      source._record.properties[key] = properties[key]
    delete source._record.properties.id
  copy = {} <<< source._record
  delete copy.qualities
  id = delete copy.id
  return wrap(source._table, first(yield exec connection, "UPDATE \"#{source._table}\" SET " + (keys copy |> map -> "\"#{camelize it}\" = ?").join(', ') + " WHERE id = ? RETURNING *", (values copy) ++ [ id ]))

camelized = {}

tables = (connection) ->*
  return ((yield exec connection, """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
    ORDER BY table_name;
  """) |> map ->
    if /\-/.test dasherize(it.table_name)
      camelized[dasherize it.table_name] = it.table_name
    else
      camelized["#{it.table_name}-id"] = "#{it.table_name}Id"
    it.table_name)

columns = (connection, table) ->*
  columns[table] = ((yield exec connection, """
    SELECT attrelid::regclass, attnum, attname
    FROM   pg_attribute
    WHERE  attrelid = '"public"."#{table}"'::regclass
    AND    attnum > 0
    AND    NOT attisdropped
    ORDER  BY attnum
  """) |> map ->
    camelized[dasherize it.attname] = it.attname if /\-/.test dasherize(it.attname)
    it.attname)

wrap = (table, record) ->
  return null if not record
  # XXX: (postgresql-9.4) Shim because node-postgres isn't parsing these out for jsonb columns
  # record.properties = JSON.parse record.properties if record.properties and typeof! record.properties == 'String'
  # record.qualities  = JSON.parse record.qualities  if record.qualities  and typeof! record.qualities  == 'String'
  extra = pairs-to-obj(keys record |> (filter -> it not in columns[table] ++ <[ properties qualities ]>) |> map -> [ it, record[it] ])
  target = ^^record
  target.toJSON = ->
    obj = pairs-to-obj(columns[table] |> (filter -> it not in <[ id properties qualities ]>) |> map -> [ it, record[it] ])
    obj[table + 'Id'] = record.id
    obj <<< record.properties or {}
    obj <<< record.qualities  or {}
    obj <<< extra
    obj
  target.inspect = -> record
  new Proxy target, do
    get: (target, name, receiver) ->
      switch
      | name == '_table'                                              => table
      | name == '_record'                                             => record
      | name in columns[table]                                        => record[name]
      | record.properties and record.properties.has-own-property name => record.properties[name]
      | record.qualities  and record.qualities.has-own-property name  => record.qualities[name]
      | otherwise                                                     => target[name] or extra[name]
    set: (target, name, val, receiver) ->
      switch
      | name in columns[table]                                        => record[name] = val
      | record.properties and record.properties.has-own-property name => record.properties[name] = val
      | record.qualities  and record.qualities.has-own-property name  => record.qualities[name]  = val
      | otherwise                                                     => extra[name] = val

setup-interface = (connection, release) ->*
  model = {}
  setup-model = (table) ->
    # Model function creates or loads records
    model[table] = (record = {}) ->*
      if typeof! record == 'String'
        return wrap(table, (yield exec-first connection, """SELECT * FROM "#table" WHERE id = ?""", record))
      else if record.id and original = wrap(table, (yield exec-first connection, """SELECT * FROM "#table" WHERE id = ?""", record.id))
        yield save connection, original, record
        return original <<< record
      else
        cols = columns[table] |> filter -> record.has-own-property(it) or (it in [ 'qualities', 'properties' ])
        if cols.length
          statement = """INSERT INTO "#table" ("#{cols.join('","')}") VALUES (#{(['?'] * cols.length).join(',')}) RETURNING *"""
        else
          statement = """INSERT INTO "#table" DEFAULT VALUES RETURNING *"""
        values = cols |> map ->
          return (record[it]) if it not in [ 'qualities', 'properties' ]
          extra = {}
          keys record
          |> filter -> it[0] != '_' and (it not in cols)
          |> each   -> extra[it] = record[it]
          JSON.stringify(extra)
        return wrap(table, (yield exec connection, statement, values)[0])
    find-statement = (query = {}) ->
      statement = knex(table).select '*'
      vals = []
      keys query |> each ->
        if it in columns[table]
          (typeof! query[it] == 'Array' and statement.where-in it, query[it]) or statement.where it, query[it]
        else
          if query[it] == true
            statement.where-raw "\"#table\".properties ?? '#it'"
          else if not query[it]
            statement.where-raw "NOT (\"#table\".properties ?? '#it')"
          else
            statement.where-raw "\"#table\".properties ->> '#it' = ?"
            vals.push query[it]
      [ statement, vals ]
    model[table].find = (query = {}) ->*
      [ statement, vals ] = find-statement query
      records = yield exec connection, statement, vals
      return (records |> map -> wrap(table, it))
    model[table].find-first = (query = {}) ->*
      [ statement, vals ] = find-statement query
      record = yield exec-first connection, statement, vals
      return (record and wrap(table, record)) or null
  for table in (yield tables connection)
    yield columns connection, table if not columns[table]
    setup-model table
  return do
    release: release
    model: model
    error: -> @_error = it if it; connection.error or @_error
    exec: (statement, ...args) -> exec(connection, statement, ...args)
    first: (statement, ...args) -> exec-first(connection, statement, ...args)
    relate: (source, target, qualities = {}) ->*
      for key of qualities
        qualities[key] = undefined if qualities[key] == false
      join-table = camelize (sort [source._table, target._table]).join('-')
      source-id = (source._table == target._table and 'sourceId') or source._table + 'Id'
      target-id = (source._table == target._table and 'targetId') or target._table + 'Id'
      statement = """SELECT * FROM "#join-table" WHERE "#source-id" = ? AND "#target-id" = ?"""
      join-record = yield exec-first connection, statement, source.id, target.id
      if not join-record
        statement = """INSERT INTO "#join-table" ("#source-id", "#target-id") VALUES (?, ?) RETURNING *"""
        join-record = first(yield exec connection, statement, source.id, target.id)
      join-record.qualities <<< qualities
      statement = """UPDATE "#join-table" SET qualities = ? WHERE "#source-id" = ? AND "#target-id" = ?"""
      statement += " RETURNING *"
      record = first (yield exec connection, statement, JSON.stringify(join-record.qualities), source.id, target.id)
      record.id = delete record[source-id]
      return @wrap source._table, record
    estrange: (source, target) ->*
      join-table = camelize (sort [source._table, target._table]).join('-')
      source-id = (source._table == target._table and 'sourceId') or source._table + 'Id'
      target-id = (source._table == target._table and 'targetId') or target._table + 'Id'
      statement = """DELETE FROM "#join-table" WHERE "#source-id" = ? AND "#target-id" = ?"""
      yield exec connection, statement, source.id, target.id
    related: (source, target, properties = {}, qualities = {}) ->*
      source = { _table: source } if typeof! source == 'String'
      target = { _table: target } if typeof! target == 'String'
      join-table = camelize (sort [source._table, target._table]).join('-')
      source-id = (source._table == target._table and 'sourceId') or source._table + 'Id'
      target-id = (source._table == target._table and 'targetId') or target._table + 'Id'
      statement = knex(join-table)
      if source.id and target.id
        statement.where (source-id): source.id
        statement.where (target-id): target.id
        statement.select 'qualities'
      else if source.id
        statement.join target._table, "#join-table.#target-id", "#{target._table}.id"
        statement.where (source-id): source.id
        keys properties |> each ->
          if it in columns[table]
            (typeof! properties[it] == 'Array' and statement.where-in "#{target._table}.#it", properties[it]) or statement.where "#{target._table}.#it", properties[it]
          else if typeof! properties[it] == 'Array'
            statement.where-raw "\"#{target._table}\".properties ->> '#it' in (#{(['?'] * properties[it].length).join(',')})"
          else if typeof! properties[it] == 'Undefined' or properties[it] == false
            statement.where-raw "(\"#{target._table}\".properties ?? '#it') = false"
            delete properties[it]
          else if properties[it] == true
            statement.where-raw "(\"#{target._table}\".properties ?? '#it') = true"
            delete properties[it]
          else
            statement.where-raw "\"#{target._table}\".properties ->> '#it' = ?"
        keys qualities |> each ->
          if typeof! qualities[it] == 'Array'
            statement.where-raw "\"#join-table\".qualities ->> '#it' in (#{(['?'] * properties[it].length).join(',')})"
          else if typeof! qualities[it] == 'Undefined' or qualities[it] == false
            statement.where-raw "(\"#join-table\".qualities ?? '#it') = false"
            delete qualities[it]
          else if qualities[it] == true
            statement.where-raw "(\"#join-table\".qualities ?? '#it') = true"
            delete qualities[it]
          else
            statement.where-raw "\"#join-table\".qualities ->> '#it' = ?"
        statement.select "qualities", "#{target._table}.*"
      else
        statement.join source._table, "#join-table.#source-id", "#{source._table}.id"
        statement.where (target-id): target.id
        keys properties |> each ->
          if it in columns[table]
            (typeof! properties[it] == 'Array' and statement.where-in "#{source._table}.#it", properties[it]) or statement.where "#{source._table}.#it", properties[it]
          else if typeof! properties[it] == 'Array'
            statement.where-raw "\"#{source._table}\".properties ->> '#it' in (#{(['?'] * properties[it].length).join(',')})"
          else if typeof! properties[it] == 'Undefined' or properties[it] == false
            statement.where-raw "(\"#{source._table}\".properties ?? '#it') = false"
            delete properties[it]
          else if properties[it] == true
            statement.where-raw "(\"#{source._table}\".properties ?? '#it') = true"
            delete properties[it]
          else
            statement.where-raw "\"#{source._table}\".properties ->> '#it' = ?"
        keys qualities |> each ->
          if typeof! qualities[it] == 'Array'
            statement.where-raw "\"#join-table\".qualities ->> '#it' in (#{(['?'] * properties[it].length).join(',')})"
          else if typeof! qualities[it] == 'Undefined' or qualities[it] == false
            statement.where-raw "(\"#join-table\".qualities ?? '#it') = false"
            delete qualities[it]
          else if qualities[it] == true
            statement.where-raw "(\"#join-table\".qualities ?? '#it') = true"
            delete qualities[it]
          else
            statement.where-raw "\"#join-table\".qualities ->> '#it' = ?"
        statement.select "qualities", "#{source._table}.*"
      records = yield exec connection, statement, flatten (keys properties |> (filter -> it not in columns[table]) |> map -> properties[it]) ++ (values qualities)
      if source.id and target.id
        return [] if not records.length
        return [records[0].qualities]
      if source.id
        return (records |> map -> wrap(target._table, it))
      else
        return (records |> map -> wrap(source._table, it))
    relation: (source, target, properties = {}, qualities = {}) ->*
      return first (yield @related source, target, properties, qualities)
    save: (source, properties = {}) ->*
      return (yield save connection, source, properties)
    destroy: (record) ->*
      yield exec connection, "DELETE FROM #{record._table} where id = ?", record.id
    wrap: (table, records) ->
      if typeof! records == 'Array'
        records |> map -> wrap table, it
      else
        wrap table, records

export connect-pool = (url) ->*
  [ connection, release ] = yield pg.connect-async url
  return yield setup-interface connection, release

export connect = (url, single) ->*
  client = new pg.Client url
  yield client.connect-async!
  return yield setup-interface client, -> client.end!
