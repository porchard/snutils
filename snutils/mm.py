#!/usr/bin/env python

import pandas as pd
import re

def _validate_mm(df, features, barcodes):
    assert(isinstance(df, pd.DataFrame))
    assert(isinstance(features, list))
    assert(isinstance(barcodes, list))
    assert('feature' in df.columns)
    assert('barcode' in df.columns)
    assert('count' in df.columns)
    assert(pd.Series(features).value_counts().max()==1)
    assert(pd.Series(barcodes).value_counts().max()==1)
    assert(all(df.feature.isin(features)))
    assert(all(df.barcode.isin(barcodes)))
    return True


def read_mm(matrix_file, features_file, barcodes_file):
    df = pd.read_csv(matrix_file, delim_whitespace=True, skiprows=3, header=None, names=['feature_idx', 'barcode_idx', 'count'])
    features = pd.read_csv(features_file, sep='\t', header=None).apply(lambda x: '\t'.join(x), axis=1)
    barcodes = pd.read_csv(barcodes_file, sep='\t', header=None).apply(lambda x: '\t'.join(x), axis=1)
    features.index = features.index + 1 # because mm indexes from 1
    barcodes.index = barcodes.index + 1 # because mm indexes from 1
    df['feature'] = df.feature_idx.map(features.to_dict())
    df['barcode'] = df.barcode_idx.map(barcodes.to_dict())
    features = features.to_list()
    barcodes = barcodes.to_list()
    _validate_mm(df, features, barcodes)
    return (df[['feature', 'barcode', 'count']], features, barcodes)


def write_mm(df, features, barcodes, prefix=''):
    """
    df should be Pandas dataframe with columns 'feature', 'barcode', 'count'
    features should be a list of *all* features that *could* have counts
    barcodes should be a list of *all* barcodes that *could* have counts
    """
    _validate_mm(df, features, barcodes)
    
    matrix_file = f'{prefix}matrix.mtx'
    feature_file = f'{prefix}features.tsv'
    barcode_file = f'{prefix}barcodes.tsv'
    
    feature_to_idx = {v: k for k, v in dict(enumerate(features, 1)).items()}
    barcode_to_idx = {v: k for k, v in dict(enumerate(barcodes, 1)).items()}
    
    new_df = df.loc[:,['feature', 'barcode', 'count']]
    new_df['feature_idx'] = new_df.feature.map(feature_to_idx)
    new_df['barcode_idx'] = new_df.barcode.map(barcode_to_idx)
    
    with open(matrix_file, 'w') as f:
        f.write('%%MatrixMarket matrix coordinate integer general\n')
        f.write('%\n')
        f.write('{} {} {}\n'.format(len(features), len(barcodes), len(df)))
        new_df[['feature_idx', 'barcode_idx', 'count']].to_csv(f, sep=' ', index=False, header=False)
    
    pd.DataFrame({'x': features}).to_csv(feature_file, index=False, header=False)
    pd.DataFrame({'x': barcodes}).to_csv(barcode_file, index=False, header=False)
    
    return (matrix_file, feature_file, barcode_file)


def get_total_counts_mm(matrix_file, features_file, barcodes_file):
    """
    returns: tuple.
    first element is a dict of features --> total counts.
    second element is a dict of barcodes --> total counts.
    """
    features = pd.read_csv(features_file, sep='\t', header=None).apply(lambda x: '\t'.join(x), axis=1)
    features.index = features.index + 1 # because mm indexes from 1
    features = features.to_dict()
    
    barcodes = pd.read_csv(barcodes_file, sep='\t', header=None).apply(lambda x: '\t'.join(x), axis=1)
    barcodes.index = barcodes.index + 1 # because mm indexes from 1
    barcodes = barcodes.to_dict()
    
    feature_count = {v: 0 for v in features.values()}
    barcode_count = {v: 0 for v in barcodes.values()}
    line_count = 0

    with open(matrix_file, 'r') as f:
        for line in f:
            if re.match('^%', line):
                continue
            line_count += 1
            if line_count == 1:
                # header still
                continue
            feature_idx, barcode_idx, count = line.rstrip().split()
            feature_idx = int(feature_idx)
            barcode_idx = int(barcode_idx)
            count = int(count)
            feature = features[feature_idx]
            barcode = barcodes[barcode_idx]
            feature_count[feature] += count
            barcode_count[barcode] += count
    
    return (feature_count, barcode_count)


def remap_features(df, features, barcodes, old_feature_to_new_feature):
    _validate_mm(df, features, barcodes)
    assert(isinstance(old_feature_to_new_feature, dict))
    assert(all(pd.Series(features).isin(list(old_feature_to_new_feature.keys()))))
    
    new_df = df.loc[:,['feature', 'barcode', 'count']]
    new_df.feature = new_df.feature.map(old_feature_to_new_feature)
    new_df = new_df[['feature', 'barcode', 'count']].groupby(['barcode', 'feature']).sum().reset_index()
    
    new_features = pd.Series(features).map(old_feature_to_new_feature).unique().tolist()
    _validate_mm(new_df, new_features, barcodes)
    
    return (new_df, new_features, barcodes)
    

def mm_merge(matrix_list, feature_list, barcode_list):
    assert(len(matrix_list) == len(feature_list))
    assert(len(matrix_list) == len(barcode_list))
    assert(len(matrix_list) > 0)
    if len(matrix_list) == 1:
        return (matrix_list[0], feature_list[0], barcode_list[0])
    else:
        # verify all the feature lists are the same
        feature_set_1 = set(feature_list[0])
        for i in range(1, len(feature_list)):
            assert(feature_set_1 == set(feature_list[i]))
        
        # verify all the barcode lists are unique
        x = set()
        for i in barcode_list:
            x = x.union(set(i))
        assert(len(x) == sum([len(i) for i in barcode_list]))
        
        # merge
        mtx = pd.concat(matrix_list)
        new_barcodes = []
        
        for i in barcode_list:
            new_barcodes = new_barcodes + i
        
        return (mtx, feature_list[0].copy(), new_barcodes)


def mm_to_wide(mtx, features, barcodes, keep_features=None, keep_barcodes=None):
    _validate_mm(mtx, features, barcodes)
    keep_features = features.copy() if keep_features is None else keep_features
    keep_barcodes = barcodes.copy() if keep_barcodes is None else keep_barcodes
    assert(all(pd.Series(keep_features).isin(features)))
    assert(all(pd.Series(keep_barcodes).isin(barcodes)))
    # TODO: If keeping e.g. all of the features and all of the barcodes, might be slow. in 
    # that case, should pivot first, then complete the matrix
    # actually: should use np.zeroes first, then fill
    tmp = pd.DataFrame([[i, j] for i in keep_barcodes for j in keep_features], columns=['barcode', 'feature'])
    tmp = tmp.merge(mtx[(mtx.feature.isin(keep_features)) & (mtx.barcode.isin(barcodes))], how='left', on=['barcode', 'feature']).fillna(0)
    tmp['count'] = tmp['count'].astype(mtx['count'].dtype)
    return tmp[['feature', 'barcode', 'count']].pivot(index='barcode', columns='feature', values='count')