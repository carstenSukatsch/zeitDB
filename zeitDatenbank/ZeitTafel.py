#
#
#
#

import datetime
import os.path
import pandas               as pd
# import matplotlib.pyplot    as plt




class ZeitTafel:

    ##########################################################################
    ## konstanten um änderungen am aussehen der zeit tabelle zu machen.
    ##
    ZEITFMT    = '%Y-%m-%d %H:%M:%S'
    COLNAME    = 'timestamp'


    ###@staticmethod
    ###def extenderPNG( dateiname ):
    ###    ff = os.path.splitext( dateiname )
    ###    return ff[0] + ".png"


    ##########################################################################
    ## rückgabe einiger einfacher werte als property
    @property
    def df( self ):
        return self.__df


    @property
    def dateinameZeitTabelle( self ):
        return self.__csvDatei


    ##
    ##########################################################################
    ##########################################################################
    ##
    ##   dateinameZeitTabelle ->    dateiname für eine CSV datei, welche zeitaden
    ##                              enthalten soll. kann eine leere datei sein,
    ##                              dann wird ein neues objekt angelegt
    ##   deltaZeit            ->    typ: datetime.timedelta
    ##                              zeitspanne, wie lange datne in die vergangenheit hin
    ##                              behalten werden solln
    ##   zeitStempelJETZT     ->    typ: datetime.datetime
    ##                              das 'jetzt', ab dem die zeit 'deltaZeit' zurückgerecnet
    ##                              wird

    def __init__( self,
                   dateinameZeitTabelle,
                   zeitStempelJETZT,
                   data,
                   deltaZeit ):

        ##
        ## lokale daten speichern
        self.__csvDatei         = dateinameZeitTabelle
        self.__zeitStempelJETZT = zeitStempelJETZT

        #print( " --> ZeitTafel.init " , data )

        ##
        ## df aufsetzen ... entweder vom datensatz data wenn es keine CSV datei gibt
        ## oder eben von der CSV datei. hier KEIN index setzen, der scheint
        ## schlecht für die späteren arbeiten zu sein...
        if os.path.isfile( self.__csvDatei ):
            self.__df = pd.read_csv( self.__csvDatei )
        else:
            tk = [ ZeitTafel.COLNAME, *data.keys()  ]
            self.__df = pd.DataFrame( columns = tk )

        ##
        ## neuen datensatz zufügen
        zeitStempelStr = self.__zeitStempelJETZT.strftime( ZeitTafel.ZEITFMT )
        data[ ZeitTafel.COLNAME ] = zeitStempelStr
        ## function 'append' ist deprecated, so sagt mir pandas.
        ## also erstezt mit pd.concat, das aber einen DataFrame benötigt
        ##self.__df = self.__df.append( data, ignore_index=True  )
        ##
        dfData    = pd.DataFrame( [data] )
        self.__df = pd.concat( [ self.__df, dfData ], ignore_index=True )

        ##
        ## zeilen älter als das verfallsdatum entfernen
        allesWegVor    = self.__zeitStempelJETZT - deltaZeit
        allesWegVorStr = allesWegVor.strftime( ZeitTafel.ZEITFMT )
        self.__df      = self.__df[ ~( self.__df[ ZeitTafel.COLNAME ] < allesWegVorStr ) ]

        #
        # index setzen, damit die graphik eine schöne zeitleiste bekommt
        self.__df[ ZeitTafel.COLNAME ] = pd.to_datetime( self.__df[ ZeitTafel.COLNAME ] )
        self.__df.set_index( ZeitTafel.COLNAME, inplace = True )


    ##
    ##########################################################################
    ##########################################################################
    ##

    def abspeichern( self ):
        self.__df.to_csv( self.__csvDatei )
        return self.__csvDatei


    def abspeichernEXCEL( self ):
        ff = os.path.splitext( self.__csvDatei )
        e = ff[0] + ".xlsx"
        self.__df.to_excel( e )
        return e

    def abspeichernPICKLE( self ):
        ff = os.path.splitext( self.__csvDatei )
        e = ff[0] + ".pkl"
        self.__df.to_pickle( e )
        return e

    def abspeichernJSON( self ):
        ff = os.path.splitext( self.__csvDatei )
        e = ff[0] + ".json"
        self.__df.to_json( e )
        return e



    #
    ##########################################################################
    #
    # abspeichern eines DFs, wobei das format über den parameter 'formatName'
    # gegeben wird. zur zeit wird nur CSV & EXCEL unterstützt.
    #
    def abspeichernFmnt( self, formatName ):
        formatName = formatName.casefold()
        if formatName == 'excel'.casefold()     or formatName == 'xlsx'.casefold() :
            e = self.abspeichernEXCEL( )
        elif formatName == 'pickle'.casefold()  or formatName == 'pkl'.casefold() :
            e = self.abspeichernPICKLE( )
        elif formatName == 'json'.casefold():
            e = self.abspeichernJSON( )
        elif formatName == 'csv'.casefold():
            e = self.abspeichern( )
        else:
            raise NotImplementedError( "unbekanntes ausgabeformat: '{0}'.".format( formatName )  )
        return e


    ##
    #######################################################################
    ## der unvermeidliche 'print' operator
    ##
    def __repr__( self ) -> str:
        return '(ZeitTafel: datei: {csv};  timeStamp: {zeit})'.format( csv=self.__csvDatei, zeit=self.__zeitStempelJETZT  )



##
#############################################################################
#############################################################################

if __name__ == '__main__':

    from random import randrange

    testData        = { 'a' : randrange(10),
                        'b' : randrange(10),
                        'c' : randrange(10) }


    z = ZeitTafel( dateinameZeitTabelle = "t1.csv",
                   zeitStempelJETZT     = datetime.datetime.now(),
                   data                 = testData,
                   deltaZeit            = datetime.timedelta( minutes=10 )  )



    print( z )
    z.abspeichern()
    print( z.df )
    z.abspeichernFmnt( 'Excel' )
    z.abspeichernFmnt( 'pkl' )
    z.abspeichernFmnt( 'json' )

    try:
        z.abspeichernFmnt( 'dd' )
    except  NotImplementedError as err:
        print( "erfolgreich NotImplementedError gefangen: {}".format( err ) )

    print("normales ende")
