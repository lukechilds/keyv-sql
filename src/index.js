'use strict';

const EventEmitter = require('events');
const Sql = require('sql').Sql;

class KeyvSql extends EventEmitter {
	constructor(opts) {
		super();
		this.ttlSupport = false;

		opts = Object.assign({ table: 'keyv' }, opts);

		this.sql = new Sql(opts.dialect);

		this.entry = this.sql.define({
			name: opts.table,
			columns: [
				{
					name: 'key',
					dataType: 'VARCHAR(255)'
				},
				{
					name: 'value',
					dataType: 'TEXT'
				}
			]
		});
		const createTable = this.entry.create().ifNotExists().toString();

		this.connected = opts.connect()
			.then(query => query(createTable).then(() => query))
			.catch(err => this.emit('error', err));
	}

	get(key) {
		return this.connected
			.then(() => this.Entry.findById(key))
			.then(data => {
				if (data === null) {
					return undefined;
				}
				return data.get('value');
			});
	}

	set(key, value) {
		return this.connected
			.then(() => this.Entry.upsert({ key, value }));
	}

	delete(key) {
		return this.connected
			.then(() => this.Entry.destroy({ where: { key } }))
			.then(items => items > 0);
	}

	clear() {
		return this.connected
			.then(() => this.Entry.destroy({
				where: {
					key: { $like: `${this.namespace}:%` }
				}
			}))
			.then(() => undefined);
	}
}

module.exports = KeyvSql;
