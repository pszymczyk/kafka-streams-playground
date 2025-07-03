package com.pszymczyk.app5


import com.pszymczyk.common.Message
import com.pszymczyk.common.MessageSerde
import com.pszymczyk.common.User
import com.pszymczyk.common.UserSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import spock.lang.Specification

class App5UnitSpec extends Specification {

    def "Should enrich messages with full user name"() {
        given:
            def properties
            def topologyTestDriver
            def source
            def state
            def sink
        when:

        then:
            sink.readValuesToList() == ["Hello Pawel Szymczyk! how are you?",
                                        "ping",
                                        "Hi Andrzej Golara, we have some speciall offer for you"]
        and:
            sink.readKeyValuesToList() == []
        cleanup:
            topologyTestDriver.close()
    }
}
