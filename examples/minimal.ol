include "console.iol"
include "json_utils.iol"

main {
    println@Console(args[0])()
    getJsonValue@JsonUtils(args[0])(message)
    getJsonString@JsonUtils(message)(encoded)
    println@Console( encoded )(  )
}
