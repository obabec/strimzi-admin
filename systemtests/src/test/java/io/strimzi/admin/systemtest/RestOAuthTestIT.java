/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.admin.systemtest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Arquillian.class)
public class RestOAuthTestIT {
    protected static final Logger LOGGER = LogManager.getLogger(RestOAuthTestIT.class);

    @Test
    public void simplePOCTest() {
        LOGGER.info("test");
        assertThat(1).isEqualTo(1);
    }
}
