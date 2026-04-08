package lk.rumex.rumex_ott_mediaStat.config;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;

public class LenientJsonRedisSerializer implements RedisSerializer<Object> {

    private final GenericJackson2JsonRedisSerializer typedSerializer;
    private final Jackson2JsonRedisSerializer<Object> plainSerializer;

    public LenientJsonRedisSerializer() {
        ObjectMapper typedMapper = new ObjectMapper();
        typedMapper.findAndRegisterModules();
        typedMapper.activateDefaultTyping(
                LaissezFaireSubTypeValidator.instance,
                ObjectMapper.DefaultTyping.NON_FINAL,
                JsonTypeInfo.As.PROPERTY
        );
        this.typedSerializer = new GenericJackson2JsonRedisSerializer(typedMapper);

        ObjectMapper plainMapper = new ObjectMapper();
        plainMapper.findAndRegisterModules();
        Jackson2JsonRedisSerializer<Object> serializer = new Jackson2JsonRedisSerializer<>(Object.class);
        serializer.setObjectMapper(plainMapper);
        this.plainSerializer = serializer;
    }

    @Override
    public byte[] serialize(Object value) throws SerializationException {
        return typedSerializer.serialize(value);
    }

    @Override
    public Object deserialize(byte[] bytes) throws SerializationException {
        try {
            return typedSerializer.deserialize(bytes);
        } catch (SerializationException ex) {
            return plainSerializer.deserialize(bytes);
        }
    }
}
