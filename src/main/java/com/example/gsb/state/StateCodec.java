package com.example.gsb.state;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * 状态 key/value 与快照文件字节之间的编解码器。
 *
 * <p>实现必须保证 {@code decode(encode(x)).equals(x)}。快照在后台线程中编码，
 * 因此 value 应当是不可变对象（如 String、记录类），否则在快照期间被原地修改
 * 会导致快照内容不确定。
 */
public interface StateCodec<T> {

    byte[] encode(T value);

    T decode(byte[] bytes);

    /** UTF-8 字符串编解码器，适用于大多数以字符串为 key/value 的场景。 */
    static StateCodec<String> utf8String() {
        return new StateCodec<>() {
            @Override
            public byte[] encode(String value) {
                return value.getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public String decode(byte[] bytes) {
                return new String(bytes, StandardCharsets.UTF_8);
            }
        };
    }

    /** 有符号 64 位整数编解码器。 */
    static StateCodec<Long> int64() {
        return new StateCodec<>() {
            @Override
            public byte[] encode(Long value) {
                return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
            }

            @Override
            public Long decode(byte[] bytes) {
                return ByteBuffer.wrap(bytes).getLong();
            }
        };
    }
}
