-- Create indices for the tprincipals table
CREATE INDEX idx_tprincipals_nconst ON public.tprincipals (nconst);
CREATE INDEX idx_tprincipals_tconst ON public.tprincipals (tconst);
CREATE INDEX idx_tprincipals_nconst_category ON public.tprincipals (nconst, category);

-- Create indices for the nbasics table
CREATE INDEX idx_nbasics_nconst ON public.nbasics (nconst);
CREATE INDEX idx_nbasics_primaryname ON public.nbasics (primaryname);

-- Create indices for the tmovies table
CREATE INDEX idx_tmovies_tconst ON public.tmovies (tconst);
CREATE INDEX idx_tmovies_startYear ON public.tmovies (startYear);
CREATE INDEX idx_tmovies_primaryTitle ON public.tmovies (primaryTitle);

-- If needed, create indices for the tratings table
CREATE INDEX idx_tratings_tconst ON public.tratings (tconst);