package dk.statsbiblioteket.newspaper.bitrepository.ingester;


import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.util.Arrays;

import org.testng.annotations.Test;

import dk.statsbiblioteket.doms.central.CentralWebservice;
import dk.statsbiblioteket.doms.central.InvalidCredentialsException;
import dk.statsbiblioteket.doms.central.InvalidResourceException;
import dk.statsbiblioteket.doms.central.MethodFailedException;

public class DomsJP2FileUrlRegisterTest {
    public static final String FILE_URL = "http://bitfinder.statsbiblioteket.dk/newspaper/foo";
    public static final String FILE_NAME = "foo";
    public static final String FILE_PATH = "B400022028241-RT1/400022028241-14/1795-06-13-01/foo";

    @Test
    public void goodCaseRegistrationTest() throws IOException, InvalidCredentialsException, 
            MethodFailedException, InvalidResourceException {
        CentralWebservice mockCentral = mock(CentralWebservice.class);
        String TEST_PID = "pidA";
        when(mockCentral.findObjectFromDCIdentifier(anyString())).thenReturn(Arrays.asList(TEST_PID));
        DomsJP2FileUrlRegister register = new DomsJP2FileUrlRegister(mockCentral);
        
        register.registerJp2File(FILE_PATH, FILE_NAME, FILE_URL);
        
        verify(mockCentral).findObjectFromDCIdentifier(DomsJP2FileUrlRegister.PATH_PREFIX + FILE_PATH);
        verify(mockCentral).addFileFromPermanentURL(eq(TEST_PID), eq(FILE_NAME), anyString(), 
                eq(FILE_URL), eq(DomsJP2FileUrlRegister.JP2_FORMAT_URI), anyString());
        verifyNoMoreInteractions(mockCentral);
    }
    
    @Test
    public void multiplePidTest() throws IOException, InvalidCredentialsException, 
            MethodFailedException, InvalidResourceException {
        CentralWebservice mockCentral = mock(CentralWebservice.class);
        String TEST_PID_A = "pidA";
        String TEST_PID_B = "pidA";
        when(mockCentral.findObjectFromDCIdentifier(anyString())).thenReturn(Arrays.asList(TEST_PID_A, TEST_PID_B));
        DomsJP2FileUrlRegister register = new DomsJP2FileUrlRegister(mockCentral);

        try {
            register.registerJp2File(FILE_PATH, FILE_NAME, FILE_URL);
            fail();
        } catch (RuntimeException e) {
            // We expect this to happen.
        }
                
        verify(mockCentral).findObjectFromDCIdentifier(DomsJP2FileUrlRegister.PATH_PREFIX + FILE_PATH);
        verifyNoMoreInteractions(mockCentral);
    }
}
