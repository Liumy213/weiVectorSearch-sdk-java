package io.github.liumy213.param;

import io.github.liumy213.exception.ParamException;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

/**
 * Parameters for client connection.
 */
@Getter
@ToString
public class ConnectParam {
    private final String host;
    private final int port;

    protected ConnectParam(@NonNull Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link ConnectParam}
     */
    @Getter
    public static class Builder {
        private String host = "localhost";
        private int port = 18880;

        protected Builder() {
        }

        /**
         * Sets the host name/address.
         *
         * @param host host name/address
         * @return <code>Builder</code>
         */
        public Builder withHost(@NonNull String host) {
            this.host = host;
            return this;
        }

        /**
         * Sets the connection port. Port value must be greater than zero and less than 65536.
         *
         * @param port port value
         * @return <code>Builder</code>
         */
        public Builder withPort(int port)  {
            this.port = port;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link ConnectParam} instance.
         *
         * @return {@link ConnectParam}
         */
        public ConnectParam build() throws ParamException {
            verify();
            return new ConnectParam(this);
        }

        protected void verify() throws ParamException {
            ParamUtils.CheckNullEmptyString(host, "Host name");

            if (port < 0 || port > 0xFFFF) {
                throw new ParamException("Port is out of range!");
            }
        }
    }
}
