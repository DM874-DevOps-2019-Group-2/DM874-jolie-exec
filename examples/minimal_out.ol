include "console.iol"
include "json_utils.iol"

// This example runs on outbound messages and allows for adding eg. an signature.

execution { single } // Mandatory

type InputType: void {
    .messageBody: string
    .ownID: int
    .recipientIDs[1, *]: int 
}

type OutputType: void {
    .messageBody: string
}

main {
    getJsonValue@JsonUtils(args[0])(message)
    if (message instanceof InputType) {
        out.messageBody = message.messageBody
        getJsonString@JsonUtils(out)(encoded)
        println@Console( encoded )(  )
    } else {
        getJsonString@JsonUtils( {.messageBody = ""} )( encoded )
        println@Console( encoded )(  )
    }
}
