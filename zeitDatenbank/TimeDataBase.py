#
#   TimeDataBase.py
#
#


import datetime
import os.path
import pandas               as pd




class TimeDataBase:

    COL_KEY        = 'schluessel'
    COL_TIMESTAMP  = 'timestamp'
    ZEITFMT        = '%Y-%m-%d %H:%M:%S'

    # diese 3 formate unterstütze ich zum speichern/lesen der zeitinformation
    FMT_CSV        = 'csv'.casefold()
    FMT_PICKLE     = 'pickle'.casefold()
    FMT_FEATHER    = 'feather'.casefold()


    # recht einfaches speichern der lesefuktionen in einem dictionary.
    # kann direkt genutzt werden, das es ja keine methoden des dataframe sind
    FN_LESEN       = { FMT_CSV      : pd.read_csv,
                       FMT_PICKLE   : pd.read_pickle,
                       FMT_FEATHER  : pd.read_feather }

    # speichert die Funktionen zum schreiben als "unbound method",
    # da das dazugehörige Dataframe Objekt ja viel später erzeugt wird.
    # die schreibe-funktion muss dann per VARIABLE( self._df, paramater... )
    # aufgerufen werden
    # https://stackoverflow.com/questions/1855558/call-method-from-string
    # alternativ hätte ich den Methodennamen als String speichern können und per
    # getattr( CallMe, variable )() aufgerufen
    FN_SCHREIBEN   = { FMT_CSV      : pd.DataFrame.to_csv,
                       FMT_PICKLE   : pd.DataFrame.to_pickle,
                       FMT_FEATHER  : pd.DataFrame.to_feather }


    #
    #################################################################################
    #

    @property
    def df( self ) -> pd.DataFrame :
        return self._df

    @property
    def anzahlAnderungen( self ) -> int :
        return self.__anzahlAnderung

    @property
    def dateiName( self ) -> str :
        return self.__dateiName


    #
    #################################################################################
    #
    @staticmethod
    def _normalisiereFormat( formatString:str ) -> str :
        ff = formatString.casefold()
        for l in [ TimeDataBase.FMT_CSV,  TimeDataBase.FMT_PICKLE, TimeDataBase.FMT_FEATHER ] :
            if l == ff:
                return l
        return TimeDataBase.FMT_CSV


    #
    #################################################################################
    #
    #   abfrage, welche schlüssel alle genutzt werden, und wie oft
    #
    def genutzteSchluessel( self ) -> dict :
        return self._df[ TimeDataBase.COL_KEY  ].value_counts().to_dict()


    #
    #################################################################################
    #
    #   initialisieren des "ZeitDatenbank" Objektes
    #   dazu muss festgelegt werden, welche Datei zum persistenten speichern der Daten
    #   genutzt wird. es wird NUR der Name OHNE Erweiterung angegeben, er ergibt sich dan aus
    #   dem gewählten Format (zur zeit: csv, Pickel, feather)
    #
    #   dateiName       : string, Dateiname OHNE Erweiterung
    #   format          : eines der werte  FMT_CSV,FMT_PICKLE,FMT_FEATHER
    #   zeitZurueck,
    #   zeitJETZT       : siehe methode 'entferneAllesVor'
    #
    #   autoSave        : boolean. beeinflusst das verhalten des Dekonstruktion.
    #                     None : er macht nix
    #                     False: wird ein Objekt mit Änderungen aus dem Speicher entfernt,
    #                            so erfolgt eine Meldung
    #                     True : die Daten werden in die Datei 'dateiName' gespeichert.
    # autosave geht nicht:
    # https://stackoverflow.com/questions/67902926/python-importerror-in-del-how-to-solve-this
    #
    def __init__( self, dateiName:str, dfFormat:str = FMT_CSV,
                  zeitZurueck:datetime.timedelta = None, zeitJETZT:datetime.datetime = None, warnungUngesichert = False ):

        self.__warnungUngesichert  = warnungUngesichert
        self.__fateiFormat         = TimeDataBase._normalisiereFormat( dfFormat )
        self.__dateiName           = dateiName + '.' +  self.__fateiFormat
        self.__anzahlAnderung      = 1

        ##
        ## df aufsetzen ... entweder vom datensatz data wenn es keien CSV datei gibt
        ## opder eben von der CSV date. hier KEIN index setzen, der scheint
        ## schlecht für die späteren arbeiten zu sein...
        #print( "----<",  self.__dateiName )
        if os.path.isfile( self.__dateiName ):
            self._df = TimeDataBase.FN_LESEN[ self.__fateiFormat ]( self.__dateiName )
            #if self.__fateiFormat == TimeDataBase.FMT_PICKLE:
            #    self._df = pd.read_pickle( self.__dateiName )
            #elif self.__fateiFormat == TimeDataBase.FMT_FEATHER:
            #    self._df = pd.read_feather( self.__dateiName )
            #else:
            #    self._df = pd.read_csv( self.__dateiName )
        else:
            tk = [ TimeDataBase.COL_KEY, TimeDataBase.COL_TIMESTAMP   ]
            self._df = pd.DataFrame( columns=tk )

        # zeit-daten bereinigen, wenn daten dafür vorhanden
        if zeitZurueck is not None  and  zeitJETZT is not None:
            self.entferneAllesVor( zeitJETZT=zeitJETZT, zeitZurueck=zeitZurueck )


    #
    #################################################################################
    #
    # instanz wird entfernt... GGFLS speichern, was leider nicht zuverlässig
    # funktioniert; siehe weiter unten
    #
    def __del__( self ):
        # speicher hängt am status 'autoSave'
        #print(" time data base word dekonstruiert")

        if self.__warnungUngesichert is None or not self.__warnungUngesichert:
            return

        # das geht leider ... nicht
        # https://stackoverflow.com/questions/67902926/python-importerror-in-del-how-to-solve-this
        # if self.__autoSave and self.__anzahlAnderung > 0:
        #    self.write()

        # zur erinnerung: die funktion 'write()' setzt den wert
        # self.__anzahlAnderung auf 0!
        if self.__anzahlAnderung > 0:
            print( "WARNUNG: ungesicherte änderungen in der zeitDatenbank '{dName}' ".format( dName=self.__dateiName ) )

        #print(" fertig ")

    #
    #################################################################################
    #
    # die lösung mit der ungebundenen method ehabe ich hir her:
    # https://stackoverflow.com/questions/1855558/call-method-from-string
    #
    def write( self ):

        self.__anzahlAnderung = 0

        fUnbound = TimeDataBase.FN_SCHREIBEN[ self.__fateiFormat ]
        if self.__fateiFormat == TimeDataBase.FMT_CSV:
            fUnbound( self._df, self.__dateiName, index=False )
        else:
            fUnbound( self._df, self.__dateiName  )

    # https://stackoverflow.com/questions/66180224/can-i-call-function-in-python-with-named-arguments-as-a-variable
    # evtl lässt ssich so das if vermedden

        ##if self.__fateiFormat == TimeDataBase.FMT_PICKLE:
        ##    self._df.to_pickle( self.__dateiName )
        ##elif self.__fateiFormat == TimeDataBase.FMT_FEATHER:
        ##    self._df.to_feather( self.__dateiName )
        ##else:
        ##    self._df.to_csv( self.__dateiName, index=False  )


    #
    #################################################################################
    #
    #  zeilen in der tabelle älter als das verfallsdatum entfernen.
    #  das verfallsdatum erechnit sich aus einern 'basis'm dem jetzt' sowie einer
    #  zeitspanne hinein in  die vergangenheit.
    #
    #   zeitZurueck   ->    typ: datetime.timedelta
    #                       zeitspanne, wie lange daten in die vergangenheit hin
    #                       behalten werden sollen
    #   zeitJETZT     ->    typ: datetime.datetime
    #                       das 'jetzt', ab dem die zeit 'deltaZeit' zurückgerechnet
    #                       wird
    #
    def entferneAllesVor( self, zeitJETZT:datetime.datetime, zeitZurueck:datetime.timedelta ):

        allesWegVor    = zeitJETZT - zeitZurueck
        allesWegVorStr = allesWegVor.strftime( TimeDataBase.ZEITFMT )
        self._df       = self._df[ ~( self._df[ TimeDataBase.COL_TIMESTAMP ] < allesWegVorStr ) ]
        self.__anzahlAnderung += 1



    #
    #################################################################################
    #
    #   timeStamp   ->    typ: datetime.datetime
    #                     zeitpunkt, der für diesen datensatz gültig ist
    #   key         ->    typ: string
    #                     schlüssel für diesen datensatz, der die daten
    #                     gilt und sie gruppiert
    #
    def add( self, key:str, timeStamp:datetime.datetime, dataDict:dict ):

        self.__anzahlAnderung += 1
        ##
        ## neuen datensatz zufügen, zuerst das dictionary
        ## mit den spaltennamen für schlüssel & zeit erweitern
        zeitStempelStr = timeStamp.strftime( TimeDataBase.ZEITFMT )
        dataDict[ TimeDataBase.COL_TIMESTAMP ] = zeitStempelStr
        dataDict[ TimeDataBase.COL_KEY ]       = key
        # und das dann dem datenfelf zufügen
        dfData   = pd.DataFrame( [ dataDict ] )
        self._df = pd.concat( [ self._df, dfData ], ignore_index=True )



    #
    #################################################################################
    #
    #   key       ->   typ: string
    #                  schlüssel, der die zusammenhängende daten selektiert,
    #                  siehe funktion 'add'
    #   rückgabe  ->   ein pandas Dataframe, mit einer zeitmarke als index
    #
    def zeitTafel( self, key : str ) -> pd.DataFrame:

        # daten selektieren, gemäss 'key'
        x = self._df.loc[ self._df[ TimeDataBase.COL_KEY  ] == key ]

        # und die leeren spalten sowie die 'COL_KEY' entfernen
        x = x.dropna( axis=1, how='all'   )
        x.drop( TimeDataBase.COL_KEY, inplace=True, axis=1 )

        # index setzen, damit die graphik eine schöne zeitleiste bekommt
        x[ TimeDataBase.COL_TIMESTAMP ] = pd.to_datetime(x[ TimeDataBase.COL_TIMESTAMP ] )
        x.set_index( TimeDataBase.COL_TIMESTAMP, inplace = True )

        return x



###################################################################################
###################################################################################

if __name__ == '__main__':

    from random import randrange
    import matplotlib.pyplot  as plt
    ## import numpy              as np

    print( "-- start, initialisrung ------------------")

    # FMT_FEATHER  FMT_CSV
    tdb = TimeDataBase( dateiName = 'td', dfFormat = TimeDataBase.FMT_FEATHER  )

    print( "---------------------------------------")
    print( tdb.df  )
    print( "---------------------------------------")
    tdb.df.info( verbose=True )
    print( "---------------------------------------")

    dataDictA            = { 'a' : randrange(20),
                             'b' : randrange(20),
                             'x' : randrange(20)  }
    dataDictB            = { 'c' : randrange(20),
                             'b' : randrange(20)  }
    zeitStempelJETZT     = datetime.datetime.now()


    tdb.entferneAllesVor( zeitJETZT   = zeitStempelJETZT,
                          zeitZurueck = datetime.timedelta( minutes=15 ) )

    print( "---------------------------------------")
    print( "-- wert zufügen -----------------------")

    tdb.add( key       = '00-11-22',
             timeStamp = zeitStempelJETZT,
             dataDict  = dataDictA )

    tdb.add( key       = '00-11-33',
             timeStamp = zeitStempelJETZT,
             dataDict  = dataDictB )


    print( tdb.df  )
    tdb.write()



    print( "---------------------------------------")
    print( "-- zeitTafel holen --------------------")
    z = tdb.zeitTafel( '00-11-22' )
    print( z )
    print( "---------------------------------------")

    z.to_csv( 'zeitverlauf.csv' )

    plt.clf()
    z.plot(  )
    plt.savefig( 'linie.png' )
    plt.close( )

    hh = tdb.genutzteSchluessel()
    print( hh )

    print( " ende ...." )
