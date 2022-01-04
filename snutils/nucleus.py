#!/usr/bin/env python

import pkg_resources
import pandas as pd
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s: %(message)s')

BARCODES = None
ATAC_TO_RNA_BARCODES = None
RNA_TO_ATAC_BARCODES = None

def get_barcodes(verbose=False):
    """
    Load a pandas DataFrame of multiome barcodes (columns: atac_barcode, rna_barcode)
    """
    ATAC_BARCODES_FILE = pkg_resources.resource_filename('snutils', 'data/barcodes.atac.737K-arc-v1.txt.gz')
    RNA_BARCODES_FILE = pkg_resources.resource_filename('snutils', 'data/barcodes.rna.737K-arc-v1.txt.gz')
    # read in RNA <--> ATAC barcode pairs
    if verbose:
        logging.info(f'Reading ATAC barcodes from {ATAC_BARCODES_FILE}')
    atac_barcodes = pd.read_csv(ATAC_BARCODES_FILE, header=None, names=['atac_barcode'])
    if verbose:
        logging.info(f'Reading RNA barcodes from {RNA_BARCODES_FILE}')
    rna_barcodes = pd.read_csv(RNA_BARCODES_FILE, header=None, names=['rna_barcode'])
    barcodes = pd.concat([atac_barcodes, rna_barcodes], axis=1)
    return barcodes


def rna_to_atac_barcode(b):
    """
    Convert RNA barcode(s) to ATAC barcode(s)
    """
    global BARCODES
    global RNA_TO_ATAC_BARCODES

    if BARCODES is None:
        BARCODES = get_barcodes()
    if RNA_TO_ATAC_BARCODES is None:
        RNA_TO_ATAC_BARCODES = dict(zip(BARCODES.rna_barcode, BARCODES.atac_barcode))
    mappings = RNA_TO_ATAC_BARCODES

    if isinstance(b, str):
        return mappings[b]
    elif isinstance(b, (list, pd.Series)):
        return [mappings[i] for i in b]
    else:
        raise TypeError('b must be a string, list, or pandas Series')


def atac_to_rna_barcode(b):
    """
    Convert ATAC barcode(s) to RNA barcode(s)
    """
    global BARCODES
    global ATAC_TO_RNA_BARCODES

    if BARCODES is None:
        BARCODES = get_barcodes()
    if ATAC_TO_RNA_BARCODES is None:
        ATAC_TO_RNA_BARCODES = dict(zip(BARCODES.atac_barcode, BARCODES.rna_barcode))
    mappings = ATAC_TO_RNA_BARCODES
    
    if isinstance(b, str):
        return mappings[b]
    elif isinstance(b, (list, pd.Series)):
        return [mappings[i] for i in b]
    else:
        raise TypeError('b must be a string, list, or pandas Series')


def parse_nucleus(n):
    """
    Parse a nucleus identifier, returning it's component parts.

    Nuclei should be named:
    {sample}-{genome}-{modality}-{barcode}

    sample/genome/modality/barcode should not contain the character '-' 

    If n is a str, returns a dict.
    If n is a list/pd.Series, returns a pd.DataFrame
    """
    if isinstance(n, str):
        sample, genome, modality, barcode = n.split('-')
        return {'sample': sample, 'genome': genome, 'modality': modality, 'barcode': barcode}
    elif isinstance(n, (pd.Series, list)):
        df = n.str.split('-', expand=True) if isinstance(n, pd.Series) else pd.DataFrame([i.split('-') for i in n])
        if not len(df.columns) == 4:
            raise ValueError('Trouble parsing nuclei.')
        df.columns = ['sample', 'genome', 'modality', 'barcode']
        return df
    else:
        raise TypeError('n must be a string, list, or pandas Series.')


def atac_to_rna_nucleus(n):
    """
    Convert an ATAC nucleus (format {sample}-{genome}-ATAC-{barcode}) to the corresponding RNA nucleus.

    If n is a str, returns a str.
    If n is a list/pd.Series, returns a list.
    """
    if isinstance(n, str):
        x = parse_nucleus(n)
        if not x['modality'] == 'ATAC':
            raise TypeError(f'Modality of {n} is not ATAC')
        x['modality'] = 'RNA'
        x['barcode'] = atac_to_rna_barcode(x['barcode'])
        return '{sample}-{genome}-{modality}-{barcode}'.format(**x)
    elif isinstance(n, (pd.Series, list)):
        x = parse_nucleus(n)
        if not all(x.modality == 'ATAC'):
            raise TypeError('Not all nuclei are ATAC nuclei')
        x['modality'] = 'RNA'
        x['barcode'] = atac_to_rna_barcode(x['barcode'])
        return (x['sample'] + '-' + x['genome'] + '-' + x['modality'] + '-' + x['barcode']).to_list()
    else:
        raise TypeError('n must be a string, list, or pandas Series.')


def rna_to_atac_nucleus(n):
    """
    Convert an RNA nucleus (format {sample}-{genome}-ATAC-{barcode}) to the corresponding RNA nucleus.

    If n is a str, returns a str.
    If n is a list/pd.Series, returns a list.
    """
    if isinstance(n, str):
        x = parse_nucleus(n)
        if not x['modality'] == 'RNA':
            raise TypeError(f'Modality of {n} is not RNA')
        x['modality'] = 'ATAC'
        x['barcode'] = rna_to_atac_barcode(x['barcode'])
        return '{sample}-{genome}-{modality}-{barcode}'.format(**x)
    elif isinstance(n, (pd.Series, list)):
        x = parse_nucleus(n)
        if not all(x.modality == 'RNA'):
            raise TypeError('Not all nuclei are RNA nuclei')
        x['modality'] = 'ATAC'
        x['barcode'] = rna_to_atac_barcode(x['barcode'])
        return (x['sample'] + '-' + x['genome'] + '-' + x['modality'] + '-' + x['barcode']).to_list()
    else:
        raise TypeError('n must be a string, list, or pandas Series.')