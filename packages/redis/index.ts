import * as lodash from "lodash"
import { v4 } from "uuid"
import * as redis from "redis"

const client = redis.createClient()
export const Redis = async function (Fookie) {
    const { Text } = Fookie.Type

    return Fookie.Core.database({
        pk: "id",
        pk_type: Text,
        connect: async function () {
            return new Promise((resolve, reject) => {
                client.on("connect", () => {
                    resolve(true)
                })
                client.on("error", (error) => {
                    reject(error)
                })
            })
        },
        disconnect: async function () {
            return true
        },
        modify: function (model) {
            model.methods = {}

            model.methods.read = async function (_payload) {
                const filter = _payload.query.filter
                const attributes = ["id"].concat(_payload.query.attributes)

                const pool = await getPool(model.name)
                let res = poolFilter(pool, filter)
                res = res.map(function (entity) {
                    return lodash.pick(entity, attributes)
                })
                res = lodash.slice(res, _payload.query.offset, _payload.query.offset + _payload.query.limit)
                _payload.response.data = res
            }

            model.methods.create = async function (_payload) {
                const attributes = ["id"].concat(_payload.query.attributes)
                _payload.body.id = v4().replace("-", "")

                await setEntity(model.name, _payload.body)
                _payload.response.data = lodash.pick(_payload.body, attributes)
            }

            model.methods.update = async function (_payload) {
                const pool = await getPool(model.name)
                const ids = poolFilter(pool, _payload.query.filter).map(function (i) {
                    return i[model.database.pk]
                })
                for (const item of pool) {
                    for (const key in _payload.body) {
                        if (ids.includes(item.id)) {
                            item[key] = _payload.body[key]
                        }
                    }
                }
                await updatePool(model.name, pool)
                _payload.response.data = true
            }

            model.methods.delete = async function (_payload) {
                const pool = await getPool(model.name)
                const filtered = poolFilter(pool, _payload.query.filter).map(function (f) {
                    return f.id
                })
                const rejected = lodash.reject(pool, function (entity) {
                    return filtered.includes(entity.id)
                })
                await updatePool(model.name, rejected)
                _payload.response.data = true
            }

            model.methods.count = async function (_payload) {
                const pool = await getPool(model.name)
                _payload.response.data = poolFilter(pool, _payload.query.filter).length
            }
        },
    })
}

async function getPool(modelName) {
    return JSON.parse(await client.get(modelName))
}

async function setEntity(modelName, entity) {
    const pool = await getPool(modelName)
    pool.push(entity)
    return client.set(modelName, JSON.stringify(pool))
}

async function updatePool(modelName, pool) {
    return await client.set(modelName, JSON.stringify(pool))
}

function poolFilter(pool: any[], filter) {
    return pool.filter(function (entity) {
        for (const field in filter) {
            const value = filter[field]
            if (typeof value === "object") {
                if (value.gte && entity[field] < value.gte) {
                    return false
                }
                if (value.gt && entity[field] <= value.gt) {
                    return false
                }
                if (value.lte && entity[field] > value.lte) {
                    return false
                }
                if (value.lt && entity[field] >= value.lt) {
                    return false
                }
                if (value.inc && !entity[field].includes(value.inc)) {
                    return false
                }
                if (value.eq && entity[field] !== value.eq) {
                    return false
                }
                if (value.not && entity[field] === value.not) {
                    return false
                }
                if (value.in && !lodash.includes(value.in, entity[field])) {
                    return false
                }
                if (value.not_in && lodash.includes(value.not_in, entity[field])) {
                    return false
                }
            } else if (entity[field] !== value) {
                return false
            }
        }
        return true
    })
}
