package dk.statsbiblioteket.newspaper.bitrepository.ingester;


import java.util.Arrays;

import dk.statsbiblioteket.doms.central.connectors.BackendInvalidCredsException;
import dk.statsbiblioteket.doms.central.connectors.BackendInvalidResourceException;
import dk.statsbiblioteket.doms.central.connectors.BackendMethodFailedException;
import dk.statsbiblioteket.doms.central.connectors.EnhancedFedora;
import org.testng.annotations.Test;

import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.fail;

public class DomsJP2FileUrlRegisterTest {
    public static final String FILE_URL = "http://bitfinder.statsbiblioteket.dk/newspaper/foo";
    public static final String FILE_NAME = "foo";
    public static final String FILE_PATH = "path:B400022028241-RT1/400022028241-14/1795-06-13-01/foo";
    public static final String CHECKSUM = "abcd";

    @Test
    public void goodCaseRegistrationTest() throws BackendInvalidCredsException, BackendMethodFailedException, 
            BackendInvalidResourceException, DomsObjectNotFoundException {
        EnhancedFedora mockCentral = mock(EnhancedFedora.class);
        String TEST_PID = "pidA";
        when(mockCentral.findObjectFromDCIdentifier(anyString())).thenReturn(Arrays.asList(TEST_PID));
        DomsJP2FileUrlRegister register = new DomsJP2FileUrlRegister(mockCentral);
        
        register.registerJp2File(FILE_PATH, FILE_NAME, FILE_URL, CHECKSUM);
        
        verify(mockCentral).findObjectFromDCIdentifier(FILE_PATH);
        verify(mockCentral).addExternalDatastream(eq(TEST_PID), eq("CONTENTS"), eq(FILE_NAME), eq(FILE_URL),
                anyString(), eq(DomsJP2FileUrlRegister.JP2_MIMETYPE), anyListOf(String.class) ,anyString());
        verify(mockCentral).addRelation(eq(TEST_PID), eq("info:fedora/" + TEST_PID + "/CONTENTS"),
                eq(DomsJP2FileUrlRegister.RELATION_PREDICATE), eq(CHECKSUM), eq(true), anyString());
        verifyNoMoreInteractions(mockCentral);
    }
    
    @Test
    public void multiplePidTest() throws BackendInvalidCredsException, BackendMethodFailedException, BackendInvalidResourceException {
        EnhancedFedora mockCentral = mock(EnhancedFedora.class);
        String TEST_PID_A = "pidA";
        String TEST_PID_B = "pidA";
        when(mockCentral.findObjectFromDCIdentifier(anyString())).thenReturn(Arrays.asList(TEST_PID_A, TEST_PID_B));
        DomsJP2FileUrlRegister register = new DomsJP2FileUrlRegister(mockCentral);

        try {
            register.registerJp2File(FILE_PATH, FILE_NAME, FILE_URL, CHECKSUM);
            fail();
        } catch (DomsObjectNotFoundException e) {
            // We expect this to happen.
        }
                
        verify(mockCentral).findObjectFromDCIdentifier(FILE_PATH);
        verifyNoMoreInteractions(mockCentral);
    }
}
